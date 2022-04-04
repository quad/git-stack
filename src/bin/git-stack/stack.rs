use std::io::Write;

use bstr::ByteSlice;
use eyre::WrapErr;
use itertools::Itertools;
use proc_exit::WithCodeResultExt;

#[derive(Debug)]
struct State {
    repo: git_stack::git::GitRepo,
    branches: git_stack::graph::BranchSet,
    head_commit: std::rc::Rc<git_stack::git::Commit>,
    stacks: Vec<StackState>,

    rebase: bool,
    pull: bool,
    push: bool,
    fixup: git_stack::config::Fixup,
    repair: bool,
    dry_run: bool,
    snapshot_capacity: Option<usize>,
    protect_commit_count: Option<usize>,
    protect_commit_age: std::time::Duration,
    protect_commit_time: std::time::SystemTime,

    show_format: git_stack::config::Format,
    show_commits: git_stack::config::ShowCommits,
    show_stacked: bool,
}

impl State {
    fn new(
        mut repo: git_stack::git::GitRepo,
        args: &crate::args::Args,
    ) -> Result<Self, proc_exit::Exit> {
        let repo_config = git_stack::config::RepoConfig::from_all(repo.raw())
            .with_code(proc_exit::Code::CONFIG_ERR)?
            .update(args.to_config());

        let mut rebase = args.rebase;
        let pull = args.pull;
        if pull {
            log::trace!("`--pull` implies `--rebase`");
            rebase = true;
        }
        let rebase = rebase;

        let fixup = match (args.fixup, args.rebase) {
            (Some(fixup), _) => fixup,
            (_, true) => repo_config.auto_fixup(),
            _ => {
                // Assume the user is only wanting to show the tree and not modify it.
                let no_op = git_stack::config::Fixup::Ignore;
                if no_op != repo_config.auto_fixup() {
                    log::trace!(
                        "Ignoring `auto-fixup={}` without an explicit `--rebase`",
                        repo_config.auto_fixup()
                    );
                }
                no_op
            }
        };
        let repair = match (args.repair(), args.rebase) {
            (Some(repair), _) => repair,
            (_, true) => repo_config.auto_repair(),
            _ => {
                // Assume the user is only wanting to show the tree and not modify it.
                if repo_config.auto_repair() {
                    log::trace!(
                        "Ignoring `auto-repair={}` without an explicit `--rebase`",
                        repo_config.auto_repair()
                    );
                }
                false
            }
        };
        let push = args.push;
        let protected = git_stack::git::ProtectedBranches::new(
            repo_config.protected_branches().iter().map(|s| s.as_str()),
        )
        .with_code(proc_exit::Code::CONFIG_ERR)?;
        let dry_run = args.dry_run;
        let snapshot_capacity = repo_config.capacity();
        let protect_commit_count = repo_config.protect_commit_count();
        let protect_commit_age = repo_config.protect_commit_age();
        let protect_commit_time = std::time::SystemTime::now() - protect_commit_age;
        let show_format = repo_config.show_format();
        let show_commits = repo_config.show_commits();
        let show_stacked = repo_config.show_stacked();

        repo.set_push_remote(repo_config.push_remote());
        repo.set_pull_remote(repo_config.pull_remote());

        let branches = git_stack::graph::BranchSet::from_repo(&repo, &protected)
            .with_code(proc_exit::Code::FAILURE)?;
        let head_commit = repo.head_commit();
        let base = args
            .base
            .as_deref()
            .map(|name| resolve_explicit_base(&repo, name))
            .transpose()
            .with_code(proc_exit::Code::USAGE_ERR)?;
        let onto = args
            .onto
            .as_deref()
            .map(|name| resolve_explicit_base(&repo, name))
            .transpose()
            .with_code(proc_exit::Code::USAGE_ERR)?;

        let stacks = match (base, onto, repo_config.stack()) {
            (Some(base), Some(onto), git_stack::config::Stack::All) => {
                vec![StackState::new(base, onto, branches.all())]
            }
            (Some(base), None, git_stack::config::Stack::All) => {
                let onto = resolve_onto_from_base(&repo, &base);
                vec![StackState::new(base, onto, branches.all())]
            }
            (None, Some(onto), git_stack::config::Stack::All) => {
                let base = resolve_base_from_onto(&repo, &onto);
                vec![StackState::new(base, onto, branches.all())]
            }
            (None, None, git_stack::config::Stack::All) => {
                let mut stack_branches = std::collections::BTreeMap::new();
                for (branch_id, branch) in branches.iter() {
                    let base_branch = resolve_implicit_base(
                        &repo,
                        branch_id,
                        &branches,
                        repo_config.auto_base_commit_count(),
                    );
                    stack_branches
                        .entry(base_branch)
                        .or_insert_with(git_stack::graph::BranchSet::default)
                        .extend(branch.iter().cloned());
                }
                stack_branches
                    .into_iter()
                    .map(|(onto, branches)| {
                        let base = resolve_base_from_onto(&repo, &onto);
                        StackState::new(base, onto, branches)
                    })
                    .collect()
            }
            (base, onto, stack) => {
                let (base, onto) = match (base, onto) {
                    (Some(base), Some(onto)) => (base, onto),
                    (Some(base), None) => {
                        let onto = resolve_onto_from_base(&repo, &base);
                        (base, onto)
                    }
                    (None, Some(onto)) => {
                        let base = resolve_implicit_base(
                            &repo,
                            head_commit.id,
                            &branches,
                            repo_config.auto_base_commit_count(),
                        );
                        // HACK: Since `base` might have come back with a remote branch, treat it as an
                        // "onto" to find the local version.
                        let base = resolve_base_from_onto(&repo, &base);
                        (base, onto)
                    }
                    (None, None) => {
                        let onto = resolve_implicit_base(
                            &repo,
                            head_commit.id,
                            &branches,
                            repo_config.auto_base_commit_count(),
                        );
                        let base = resolve_base_from_onto(&repo, &onto);
                        (base, onto)
                    }
                };
                let merge_base_oid = repo
                    .merge_base(base.id, head_commit.id)
                    .ok_or_else(|| {
                        git2::Error::new(
                            git2::ErrorCode::NotFound,
                            git2::ErrorClass::Reference,
                            format!("could not find base between {} and HEAD", base),
                        )
                    })
                    .with_code(proc_exit::Code::USAGE_ERR)?;
                let stack_branches = match stack {
                    git_stack::config::Stack::Current => {
                        branches.branch(&repo, merge_base_oid, head_commit.id)
                    }
                    git_stack::config::Stack::Dependents => {
                        branches.dependents(&repo, merge_base_oid, head_commit.id)
                    }
                    git_stack::config::Stack::Descendants => {
                        branches.descendants(&repo, merge_base_oid)
                    }
                    git_stack::config::Stack::All => unreachable!("Covered in another branch"),
                };
                vec![StackState::new(base, onto, stack_branches)]
            }
        };

        Ok(Self {
            repo,
            branches,
            head_commit,
            stacks,

            rebase,
            pull,
            push,
            fixup,
            repair,
            dry_run,
            snapshot_capacity,
            protect_commit_count,
            protect_commit_age,
            protect_commit_time,

            show_format,
            show_commits,
            show_stacked,
        })
    }

    fn update(&mut self) -> eyre::Result<()> {
        self.head_commit = self.repo.head_commit();
        self.branches.update(&self.repo)?;

        for stack in self.stacks.iter_mut() {
            stack.update(&self.repo)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct StackState {
    base: AnnotatedOid,
    onto: AnnotatedOid,
    branches: git_stack::graph::BranchSet,
}

impl StackState {
    fn new(
        base: AnnotatedOid,
        onto: AnnotatedOid,
        mut branches: git_stack::graph::BranchSet,
    ) -> Self {
        if let Some(base) = &base.branch {
            if let Some(existing) = branches
                .get_mut(base.id)
                .into_iter()
                .flat_map(|b| b.iter_mut())
                .find(|b| *b == base)
            {
                existing.set_kind(git_stack::graph::BranchKind::Protected);
            } else {
                let mut base: git_stack::graph::Branch = base.clone().into();
                base.set_kind(git_stack::graph::BranchKind::Protected);
                branches.insert(base);
            }
        }
        if let Some(onto) = &onto.branch {
            if let Some(existing) = branches
                .get_mut(onto.id)
                .into_iter()
                .flat_map(|b| b.iter_mut())
                .find(|b| *b == onto)
            {
                existing.set_kind(git_stack::graph::BranchKind::Protected);
            } else {
                let mut onto: git_stack::graph::Branch = onto.clone().into();
                onto.set_kind(git_stack::graph::BranchKind::Protected);
                branches.insert(onto);
            }
        }
        Self {
            base,
            onto,
            branches,
        }
    }

    fn update(&mut self, repo: &dyn git_stack::git::Repo) -> eyre::Result<()> {
        self.base.update(repo)?;
        self.onto.update(repo)?;
        self.branches.update(repo)?;
        Ok(())
    }
}

pub fn stack(
    args: &crate::args::Args,
    colored_stdout: bool,
    colored_stderr: bool,
) -> proc_exit::ExitResult {
    log::trace!("Initializing");
    let cwd = std::env::current_dir().with_code(proc_exit::Code::USAGE_ERR)?;
    let repo = git2::Repository::discover(&cwd).with_code(proc_exit::Code::USAGE_ERR)?;
    let repo = git_stack::git::GitRepo::new(repo);
    let mut state = State::new(repo, args)?;

    if state.pull {
        // Update status of remote unprotected branches
        let mut push_branches: Vec<_> = state
            .stacks
            .iter()
            .flat_map(|stack| stack.branches.iter())
            .flat_map(|(_, b)| b.iter())
            .filter(|b| match b.kind() {
                git_stack::graph::BranchKind::Mutable => true,
                git_stack::graph::BranchKind::Deleted
                | git_stack::graph::BranchKind::Protected
                | git_stack::graph::BranchKind::Mixed => false,
            })
            .filter_map(|b| b.push_id().and_then(|_| b.local_name()))
            .collect();
        push_branches.sort_unstable();
        if !push_branches.is_empty() {
            match git_prune_development(&mut state.repo, &push_branches, state.dry_run) {
                Ok(_) => (),
                Err(err) => {
                    log::warn!("Skipping fetch of `{}`, {}", state.repo.push_remote(), err);
                }
            }
        }

        for stack in state.stacks.iter() {
            if let Some(branch) = &stack.onto.branch {
                if let Some(remote) = &branch.remote {
                    match git_fetch_upstream(remote, branch.name.as_str()) {
                        Ok(_) => (),
                        Err(err) => {
                            log::warn!("Skipping pull of `{}`, {}", branch, err);
                        }
                    }
                } else {
                    log::warn!("Skipping pull of `{}` local branch", branch);
                }
            }
        }
        state.update().with_code(proc_exit::Code::FAILURE)?;
    }

    const STASH_STACK_NAME: &str = "git-stack";
    let mut success = true;
    let mut backed_up = false;
    let mut stash_id = None;
    if state.rebase || state.fixup != git_stack::config::Fixup::Ignore || state.repair {
        if stash_id.is_none() && !state.dry_run {
            stash_id = git_stack::git::stash_push(&mut state.repo, "branch-stash");
        }
        if state.repo.is_dirty() {
            let message = "Working tree is dirty, aborting";
            if state.dry_run {
                log::error!("{}", message);
            } else {
                git_stack::git::stash_pop(&mut state.repo, stash_id);
                return Err(proc_exit::Code::USAGE_ERR.with_message(message));
            }
        }

        {
            let stash_repo =
                git2::Repository::discover(&cwd).with_code(proc_exit::Code::USAGE_ERR)?;
            let stash_repo = git_branch_stash::GitRepo::new(stash_repo);
            let mut snapshots = git_branch_stash::Stack::new(STASH_STACK_NAME, &stash_repo);
            snapshots.capacity(state.snapshot_capacity);
            let snapshot = git_branch_stash::Snapshot::from_repo(&stash_repo)
                .with_code(proc_exit::Code::FAILURE)?;
            if !state.dry_run {
                snapshots.push(snapshot)?;
                backed_up = true;
            }
        }

        let mut head_branch = state
            .repo
            .head_branch()
            .ok_or_else(|| eyre::eyre!("Must not be in a detached HEAD state."))
            .with_code(proc_exit::Code::USAGE_ERR)?
            .name;

        let scripts: Result<Vec<_>, proc_exit::Exit> = state
            .stacks
            .iter()
            .map(|stack| {
                let script = plan_changes(&state, stack).with_code(proc_exit::Code::FAILURE)?;
                if script.is_branch_deleted(&head_branch) {
                    // Current branch is deleted, fallback to the local version of the onto branch,
                    // if possible.
                    if let Some(local_name) = stack
                        .onto
                        .branch
                        .as_ref()
                        .map(|b| b.name.as_str())
                        .filter(|n| state.repo.find_local_branch(n).is_some())
                    {
                        head_branch = local_name.to_owned();
                    }
                }
                Ok(script)
            })
            .collect();
        let scripts = scripts?;

        let mut executor = git_stack::git::Executor::new(&state.repo, state.dry_run);
        for script in scripts {
            let results = executor.run_script(&mut state.repo, &script);
            for (err, name, dependents) in results.iter() {
                success = false;
                log::error!("Failed to re-stack branch `{}`: {}", name, err);
                if !dependents.is_empty() {
                    log::error!("  Blocked dependents: {}", dependents.iter().join(", "));
                }
            }
        }
        executor
            .close(&mut state.repo, &head_branch)
            .with_code(proc_exit::Code::FAILURE)?;
        state.update().with_code(proc_exit::Code::FAILURE)?;
    }

    if state.push {
        push(&mut state).with_code(proc_exit::Code::FAILURE)?;
        state.update().with_code(proc_exit::Code::FAILURE)?;
    }

    show(&state, colored_stdout, colored_stderr).with_code(proc_exit::Code::FAILURE)?;

    git_stack::git::stash_pop(&mut state.repo, stash_id);

    if backed_up {
        let palette_stderr = if colored_stderr {
            Palette::colored()
        } else {
            Palette::plain()
        };
        log::info!(
            "{}",
            palette_stderr.hint.paint(format_args!(
                "To undo, run `git branch-stash pop {}`",
                STASH_STACK_NAME
            ))
        );
    }

    if !success {
        return proc_exit::Code::FAILURE.ok();
    }

    Ok(())
}

fn plan_changes(state: &State, stack: &StackState) -> eyre::Result<git_stack::git::Script> {
    log::trace!("Planning stack changes with base={}", stack.base);
    todo!() // TODO
}

fn push(state: &mut State) -> eyre::Result<()> {
    let mut pushable = Vec::new();
    for stack in state.stacks.iter() {
        let graphed_branches = stack.branches.clone();
        log::trace!("Calculating pushes for `{}`", stack.base);
        let graph = git_stack::graph::Graph::from_branches(&state.repo, graphed_branches)?;
        for branch in graph.branches.iter().flat_map(|(_, b)| b.iter()) {
            let push_status = graph
                .commit_get::<git_stack::graph::PushStatus>(branch.id())
                .copied()
                .unwrap_or_default();
            match push_status {
                git_stack::graph::PushStatus::Blocked(status) => {
                    log::debug!("Skipping push of `{}`: {status}", branch.display_name());
                }
                git_stack::graph::PushStatus::Pushed => {
                    log::debug!("`{}` is already up-to-date", branch.display_name());
                }
                git_stack::graph::PushStatus::Pushable => {
                    pushable.push(branch.clone());
                }
            }
        }
    }
    pushable.sort_unstable();

    let mut failed = Vec::new();
    for branch in pushable {
        if let Err(err) = git_push_branch(&mut state.repo, &branch, state.dry_run) {
            log::debug!("`git push {}` failed with {}", branch.display_name(), err);
            failed.push(branch.name());
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        eyre::bail!("Could not push {}", failed.into_iter().join(", "));
    }
}

fn show(state: &State, colored_stdout: bool, colored_stderr: bool) -> eyre::Result<()> {
    let palette_stderr = if colored_stderr {
        Palette::colored()
    } else {
        Palette::plain()
    };
    let mut empty_stacks = Vec::new();
    let mut old_stacks = Vec::new();
    let mut foreign_stacks = Vec::new();

    let abbrev_graph = match state.show_format {
        git_stack::config::Format::Silent => false,
        git_stack::config::Format::List => false,
        git_stack::config::Format::Graph => true,
        git_stack::config::Format::Debug => true,
    };

    let mut graphs = Vec::with_capacity(state.stacks.len());
    for stack in state.stacks.iter() {
        let graphed_branches = stack.branches.clone();
        if abbrev_graph && graphed_branches.len() == 1 {
            let branches = graphed_branches.iter().next().unwrap().1;
            if branches.len() == 1 && branches[0].id() != state.head_commit.id {
                empty_stacks.push(format!(
                    "{}",
                    palette_stderr.info.paint(branches[0].display_name())
                ));
                continue;
            }
        }

        log::trace!("Rendering stacks off `{}`", stack.base);
        let mut graph = git_stack::graph::Graph::from_branches(&state.repo, graphed_branches)?;
        git_stack::graph::protect_branches(&mut graph);
        if let Some(protect_commit_count) = state.protect_commit_count {
            let protected =
                git_stack::graph::protect_large_branches(&mut graph, protect_commit_count);
            if !protected.is_empty() {
                log::warn!(
                    "Branches contain more than {} commits (should these be protected?): {}",
                    protect_commit_count,
                    protected.join("m ")
                );
            }
        }
        if abbrev_graph {
            old_stacks.extend(
                git_stack::graph::trim_stale_branches(
                    &mut graph,
                    &state.repo,
                    state.protect_commit_time,
                    &[state.head_commit.id, stack.base.id, stack.onto.id],
                )
                .into_iter()
                .filter_map(|b| {
                    // Don't bother showing remote branches
                    b.local_name()
                        .map(|b| format!("{}", palette_stderr.warn.paint(b)))
                }),
            );
            if let Some(user) = state.repo.user() {
                foreign_stacks.extend(
                    git_stack::graph::trim_foreign_branches(
                        &mut graph,
                        &state.repo,
                        &user,
                        &[state.head_commit.id, stack.base.id, stack.onto.id],
                    )
                    .into_iter()
                    .filter_map(|b| {
                        // Don't bother showing remote branches
                        b.local_name()
                            .map(|b| format!("{}", palette_stderr.warn.paint(b)))
                    }),
                );
            }
        }

        if state.dry_run {
            // Show as-if we performed all mutations
            if state.rebase {
                log::trace!("Rebasing onto {}", stack.onto);
                // TODO
            }
            // TODO
            if state.repair {
                log::trace!("Repairing");
                // TODO
            }
        }

        git_stack::graph::mark_fixup(&mut graph, &state.repo);
        git_stack::graph::mark_wip(&mut graph, &state.repo);
        git_stack::graph::pushable(&mut graph);

        graphs.push(graph);
    }
    graphs.sort_by_key(|g| {
        let mut revwalk = state.repo.raw().revwalk().unwrap();
        // Reduce the number of commits to walk
        revwalk.simplify_first_parent().unwrap();
        revwalk.push(g.root_id()).unwrap();
        revwalk.count()
    });

    for (i, graph) in graphs.iter().enumerate() {
        match state.show_format {
            git_stack::config::Format::Silent => {}
            git_stack::config::Format::List => {
                let palette = if colored_stdout {
                    Palette::colored()
                } else {
                    Palette::plain()
                };
                list_stacks(&mut std::io::stdout(), &state.repo, &graph, &palette)?;
            }
            git_stack::config::Format::Graph => {
                let palette = if colored_stdout {
                    Palette::colored()
                } else {
                    Palette::plain()
                };
                if i != 0 {
                    writeln!(std::io::stdout())?;
                }
                graph_stack(
                    &mut std::io::stdout(),
                    &state.repo,
                    &graph,
                    state.show_commits,
                    state.show_stacked,
                    &palette,
                )?;
            }
            git_stack::config::Format::Debug => {
                writeln!(std::io::stdout(), "{:#?}", graph)?;
            }
        }
    }

    if !empty_stacks.is_empty() {
        log::info!("Empty stacks: {}", empty_stacks.join(", "));
    }
    if !old_stacks.is_empty() {
        log::info!(
            "Stacks older than {}: {}",
            humantime::format_duration(state.protect_commit_age),
            old_stacks.join(", ")
        );
    }
    if !foreign_stacks.is_empty() {
        log::info!("Stack from other users: {}", foreign_stacks.join(", "));
    }

    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AnnotatedOid {
    id: git2::Oid,
    branch: Option<git_stack::git::Branch>,
}

impl AnnotatedOid {
    fn new(id: git2::Oid) -> Self {
        Self { id, branch: None }
    }

    fn with_branch(branch: git_stack::git::Branch) -> Self {
        Self {
            id: branch.id,
            branch: Some(branch),
        }
    }

    fn update(&mut self, repo: &dyn git_stack::git::Repo) -> eyre::Result<()> {
        let branch = self.branch.as_ref().and_then(|branch| {
            if let Some(remote) = &branch.remote {
                repo.find_remote_branch(remote, &branch.name)
            } else {
                repo.find_local_branch(&branch.name)
            }
        });
        if let Some(branch) = branch {
            *self = Self::with_branch(branch);
        }
        Ok(())
    }
}

impl std::fmt::Display for AnnotatedOid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(branch) = &self.branch {
            branch.fmt(f)
        } else {
            self.id.fmt(f)
        }
    }
}

fn resolve_explicit_base(repo: &git_stack::git::GitRepo, base: &str) -> eyre::Result<AnnotatedOid> {
    let (obj, r) = repo.raw().revparse_ext(base)?;
    if let Some(r) = r {
        if r.is_tag() {
            return Ok(AnnotatedOid::new(obj.id()));
        }

        let branch = if r.is_remote() {
            let (remote, name) = r
                .shorthand()
                .ok_or_else(|| eyre::eyre!("Expected branch, got `{}`", base))?
                .split_once('/')
                .unwrap();
            repo.find_remote_branch(remote, name)
                .ok_or_else(|| eyre::eyre!("Could not find branch {:?}", r.shorthand()))
        } else {
            repo.find_local_branch(base)
                .ok_or_else(|| eyre::eyre!("Could not find branch {:?}", base))
        }?;
        Ok(AnnotatedOid::with_branch(branch))
    } else {
        Ok(AnnotatedOid::new(obj.id()))
    }
}

fn resolve_implicit_base(
    repo: &dyn git_stack::git::Repo,
    head_oid: git2::Oid,
    branches: &git_stack::graph::BranchSet,
    auto_base_commit_count: Option<usize>,
) -> AnnotatedOid {
    match git_stack::graph::find_protected_base(repo, branches, head_oid) {
        Some(branch) => {
            let merge_base_id = repo
                .merge_base(branch.id(), head_oid)
                .expect("to be a base, there must be a merge base");
            if let Some(max_commit_count) = auto_base_commit_count {
                let ahead_count = repo
                    .commit_count(merge_base_id, head_oid)
                    .expect("merge_base should ensure a count exists ");
                let behind_count = repo
                    .commit_count(merge_base_id, branch.id())
                    .expect("merge_base should ensure a count exists ");
                if max_commit_count <= ahead_count + behind_count {
                    let assumed_base_oid =
                        git_stack::graph::infer_base(repo, head_oid).unwrap_or(head_oid);
                    log::warn!(
                        "`{}` is {} ahead and {} behind `{}`, using `{}` as `--base` instead",
                        branches
                            .get(head_oid)
                            .map(|b| b[0].name())
                            .or_else(|| {
                                repo.find_commit(head_oid)?
                                    .summary
                                    .to_str()
                                    .ok()
                                    .map(ToOwned::to_owned)
                            })
                            .unwrap_or_else(|| "target".to_owned()),
                        ahead_count,
                        behind_count,
                        branch.display_name(),
                        assumed_base_oid
                    );
                    return AnnotatedOid::new(assumed_base_oid);
                }
            }

            log::debug!(
                "Chose branch `{}` as the base for `{}`",
                branch.display_name(),
                branches
                    .get(head_oid)
                    .map(|b| b[0].name())
                    .or_else(|| {
                        repo.find_commit(head_oid)?
                            .summary
                            .to_str()
                            .ok()
                            .map(ToOwned::to_owned)
                    })
                    .unwrap_or_else(|| "target".to_owned())
            );
            AnnotatedOid::with_branch(branch.git().to_owned())
        }
        None => {
            let assumed_base_oid = git_stack::graph::infer_base(repo, head_oid).unwrap_or(head_oid);
            log::warn!(
                "Could not find protected branch for {}, assuming {}",
                head_oid,
                assumed_base_oid
            );
            AnnotatedOid::new(assumed_base_oid)
        }
    }
}

fn resolve_base_from_onto(repo: &git_stack::git::GitRepo, onto: &AnnotatedOid) -> AnnotatedOid {
    // HACK: Assuming the local branch is the current base for all the commits
    onto.branch
        .as_ref()
        .filter(|b| b.remote.is_some())
        .and_then(|b| repo.find_local_branch(&b.name))
        .map(AnnotatedOid::with_branch)
        .unwrap_or_else(|| onto.clone())
}

fn resolve_onto_from_base(repo: &git_stack::git::GitRepo, base: &AnnotatedOid) -> AnnotatedOid {
    // HACK: Assuming the local branch is the current base for all the commits
    base.branch
        .as_ref()
        .filter(|b| b.remote.is_none())
        .and_then(|b| repo.find_remote_branch(repo.pull_remote(), &b.name))
        .map(AnnotatedOid::with_branch)
        .unwrap_or_else(|| base.clone())
}

fn git_prune_development(
    repo: &mut git_stack::git::GitRepo,
    branches: &[&str],
    dry_run: bool,
) -> eyre::Result<()> {
    if branches.is_empty() {
        return Ok(());
    }

    let remote = repo.push_remote();
    let output = std::process::Command::new("git")
        .arg("ls-remote")
        .arg("--heads")
        .arg(remote)
        .args(branches)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .wrap_err("Could not run `git fetch`")?
        .wait_with_output()?;
    if !output.status.success() {
        eyre::bail!("Could not run `git fetch`");
    }
    let stdout = String::from_utf8(output.stdout).wrap_err("Could not run `git fetch`")?;
    #[allow(clippy::needless_collect)]
    let remote_branches: Vec<_> = stdout
        .lines()
        .filter_map(|l| l.split_once('\t').map(|s| s.1))
        .filter_map(|l| l.strip_prefix("refs/heads/"))
        .collect();

    for branch in branches {
        if !remote_branches.contains(branch) {
            let remote_branch = format!("{}/{}", remote, branch);
            log::info!("Pruning {}", remote_branch);
            if !dry_run {
                let mut branch = repo
                    .raw()
                    .find_branch(&remote_branch, git2::BranchType::Remote)?;
                branch.delete()?;
            }
        }
    }

    Ok(())
}

fn git_fetch_upstream(remote: &str, branch_name: &str) -> eyre::Result<()> {
    log::debug!("git fetch {} {}", remote, branch_name);
    // A little uncertain about some of the weirder authentication needs, just deferring to `git`
    // instead of using `libgit2`
    let status = std::process::Command::new("git")
        .arg("fetch")
        .arg(remote)
        .arg(branch_name)
        .status()
        .wrap_err("Could not run `git fetch`")?;
    if !status.success() {
        eyre::bail!("`git fetch {} {}` failed", remote, branch_name,);
    }

    Ok(())
}

fn git_push_branch(
    repo: &mut git_stack::git::GitRepo,
    branch: &git_stack::graph::Branch,
    dry_run: bool,
) -> eyre::Result<()> {
    let local_branch = if let Some(local_name) = branch.local_name() {
        local_name
    } else {
        eyre::bail!("Tried to push remote branch `{}`", branch.display_name());
    };

    let raw_branch = repo
        .raw()
        .find_branch(local_branch, git2::BranchType::Local)
        .expect("all referenced branches exist");
    let upstream_set = raw_branch.upstream().is_ok();

    let remote = repo.push_remote();
    let mut args = vec!["push", "--force-with-lease"];
    if !upstream_set {
        args.push("--set-upstream");
    }
    args.push(remote);
    args.push(local_branch);
    log::trace!("git {}", args.join(" "),);
    if !dry_run {
        let status = std::process::Command::new("git").args(&args).status()?;
        match status.code() {
            Some(0) => {}
            Some(code) => {
                eyre::bail!("exit code {}", code);
            }
            None => {
                eyre::bail!("interrupted");
            }
        }
    }

    Ok(())
}

fn list_stacks(
    writer: &mut dyn std::io::Write,
    repo: &git_stack::git::GitRepo,
    graph: &git_stack::graph::Graph,
    palette: &Palette,
) -> Result<(), std::io::Error> {
    let head_branch = repo.head_branch().unwrap();
    for commit_id in graph.descendants() {
        let mut branches = graph
            .branches
            .get(commit_id)
            .map(|b| b.to_owned())
            .unwrap_or_default();
        branches.sort();
        for b in branches {
            match b.kind() {
                git_stack::graph::BranchKind::Deleted | git_stack::graph::BranchKind::Protected => {
                    // These branches are just shown for context, they aren't part of the
                    // stack, so skip them here
                    continue;
                }
                git_stack::graph::BranchKind::Mutable | git_stack::graph::BranchKind::Mixed => {}
            }
            if b.remote().is_some() {
                // Remote branches are just shown for context, they aren't part of the
                // stack, so skip them here
                continue;
            }
            writeln!(
                writer,
                "{}",
                format_branch_name(&b, graph, &head_branch, palette)
            )?;
        }
    }

    Ok(())
}

fn graph_stack(
    writer: &mut dyn std::io::Write,
    repo: &git_stack::git::GitRepo,
    graph: &git_stack::graph::Graph,
    show_commits: git_stack::config::ShowCommits,
    show_stacked: bool,
    palette: &Palette,
) -> Result<(), std::io::Error> {
    let head_branch = repo.head_branch().unwrap();
    let is_visible: Box<dyn Fn(git2::Oid) -> bool> = match show_commits {
        git_stack::config::ShowCommits::All => Box::new(|_| true),
        git_stack::config::ShowCommits::Unprotected => Box::new(|commit_id| {
            let children_count = graph.children_of(commit_id).count();
            let interesting_commit =
                commit_id == head_branch.id || commit_id == graph.root_id() || children_count == 0;
            let branches = graph.branches.get(commit_id).unwrap_or(&[]);
            let boring_commit = branches.is_empty() && children_count == 1;
            let action = graph
                .commit_get::<git_stack::graph::Action>(commit_id)
                .copied()
                .unwrap_or_default();
            let protected = action.is_protected();
            interesting_commit || !boring_commit || !protected
        }),
        git_stack::config::ShowCommits::None => Box::new(|commit_id| {
            let children_count = graph.children_of(commit_id).count();
            let interesting_commit =
                commit_id == head_branch.id || commit_id == graph.root_id() || children_count == 0;
            let branches = graph.branches.get(commit_id).unwrap_or(&[]);
            let boring_commit = branches.is_empty() && children_count == 1;
            interesting_commit || !boring_commit
        }),
    };

    let mut tree = to_tree(
        graph.root_id(),
        repo,
        graph,
        &head_branch,
        palette,
        &is_visible,
    );
    sort_tree(&mut tree);
    if show_stacked {
        linearize_tree(&mut tree);
    }
    // The tree already includes a newline
    let _ = write!(writer, "{}", tree);

    Ok(())
}

fn to_tree(
    commit_id: git2::Oid,
    repo: &git_stack::git::GitRepo,
    graph: &git_stack::graph::Graph,
    head_branch: &git_stack::git::Branch,
    palette: &Palette,
    is_visible: &dyn Fn(git2::Oid) -> bool,
) -> termtree::Tree<DisplayNode> {
    let mut tree = termtree::Tree::new(DisplayNode::new(format_commit(
        commit_id,
        repo,
        graph,
        &head_branch,
        palette,
    )));
    let default_weight = if head_branch.id == commit_id {
        Weight::Head(1)
    } else {
        let action = graph
            .commit_get::<git_stack::graph::Action>(commit_id)
            .copied()
            .unwrap_or_default();
        if action.is_protected() {
            Weight::Protected(1)
        } else {
            Weight::Commit(1)
        }
    };

    let (elide_count, commit_id) = elide_commits(commit_id, graph, is_visible);
    if let Some(elide_count) = elide_count {
        let mut child_tree =
            termtree::Tree::new(DisplayNode::new(format_elide(elide_count, palette)));
        child_tree.root.elide = true;
        child_tree.extend(
            graph
                .primary_children_of(commit_id)
                .map(|child_id| to_tree(child_id, repo, graph, head_branch, palette, is_visible)),
        );
        // HACK: We are just inheriting the weight-type of the highest child
        child_tree.root.weight = max_child_weight(&child_tree.leaves) + elide_count.get();
        tree.push(child_tree);
    } else {
        tree.extend(
            graph
                .primary_children_of(commit_id)
                .map(|child_id| to_tree(child_id, repo, graph, head_branch, palette, is_visible)),
        );
    }
    tree.root.weight = default_weight.max(max_child_weight(&tree.leaves) + 1);

    tree
}

fn max_child_weight(leaves: &[termtree::Tree<DisplayNode>]) -> Weight {
    let mut max = Weight::default();
    for leaf in leaves.iter() {
        max = leaf.root.weight.max(max);
    }
    max
}

fn elide_commits(
    mut parent_id: git2::Oid,
    graph: &git_stack::graph::Graph,
    is_visible: &dyn Fn(git2::Oid) -> bool,
) -> (Option<std::num::NonZeroUsize>, git2::Oid) {
    let original_id = parent_id;
    for elide_count in 0.. {
        if graph.primary_children_of(parent_id).count() == 1 && !is_visible(parent_id) {
            // The tree requires us to handle 0 or many children, so not checking visibility
            parent_id = graph.primary_children_of(parent_id).next().unwrap();
            continue;
        }

        return match elide_count {
            0 => {
                // Nothing elided
                (None, original_id)
            }
            1 => {
                // Not worth eliding
                (None, original_id)
            }
            _ => (std::num::NonZeroUsize::new(elide_count), parent_id),
        };
    }
    unreachable!();
}

fn sort_tree(tree: &mut termtree::Tree<DisplayNode>) {
    for child in tree.leaves.iter_mut() {
        sort_tree(child);
    }
    tree.leaves.sort_by_key(|c| c.root.weight);
}

fn linearize_tree(tree: &mut termtree::Tree<DisplayNode>) {
    if tree.root.elide {
        tree.set_glyphs(ELIDE_GLYPHS);
    } else {
        tree.set_glyphs(GLYPHS);
    }
    for child in tree.leaves.iter_mut() {
        linearize_tree(child);
    }
    if !tree.leaves.is_empty() {
        for i in 0..tree.leaves.len() {
            if i < tree.leaves.len() - 1 {
                let mut new_child =
                    termtree::Tree::new(DisplayNode::new("".to_owned())).with_glyphs(JOINT_GLYPHS);
                std::mem::swap(&mut tree.leaves[i], &mut new_child);
                let new_leaves = std::mem::take(&mut new_child.leaves);
                tree.leaves[i].leaves.push(new_child);
                tree.leaves[i].leaves.extend(new_leaves);
            } else {
                let new_leaves = std::mem::take(&mut tree.leaves[i].leaves);
                tree.leaves.extend(new_leaves);
            }
        }
    }
}

const GLYPHS: termtree::GlyphPalette = termtree::GlyphPalette {
    middle_item: "⌽",
    last_item: "⌽",
    item_indent: " ",
    skip_indent: " ",
    ..termtree::GlyphPalette::new()
};

const ELIDE_GLYPHS: termtree::GlyphPalette = termtree::GlyphPalette {
    middle_item: "┆",
    last_item: "┆",
    item_indent: " ",
    skip_indent: " ",
    ..termtree::GlyphPalette::new()
};

const SPACE_GLYPHS: termtree::GlyphPalette = termtree::GlyphPalette {
    middle_item: "│",
    last_item: " ",
    item_indent: " ",
    skip_indent: " ",
    ..termtree::GlyphPalette::new()
};

const JOINT_GLYPHS: termtree::GlyphPalette = termtree::GlyphPalette {
    item_indent: "─┐",
    ..termtree::GlyphPalette::new()
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DisplayNode {
    weight: Weight,
    display: String,
    elide: bool,
}

impl DisplayNode {
    fn new(display: String) -> Self {
        Self {
            weight: Default::default(),
            display,
            elide: false,
        }
    }
}

impl std::fmt::Display for DisplayNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.display.fmt(f)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Weight {
    Commit(usize),
    Protected(usize),
    Head(usize),
}

impl Weight {
    fn max(self, other: Self) -> Self {
        match (self, other) {
            (Self::Head(s), Self::Head(o)) => Self::Head(s.max(o)),
            (Self::Head(s), _) => Self::Head(s),
            (_, Self::Head(s)) => Self::Head(s),
            (Self::Protected(s), Self::Protected(o)) => Self::Protected(s.max(o)),
            (Self::Protected(s), _) => Self::Protected(s),
            (_, Self::Protected(o)) => Self::Protected(o),
            (Self::Commit(s), Self::Commit(o)) => Self::Commit(s.max(o)),
        }
    }
}

impl std::ops::Add for Weight {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Protected(s), Self::Protected(o)) => Self::Protected(s.saturating_add(o)),
            (Self::Protected(s), _) => Self::Protected(s),
            (_, Self::Protected(o)) => Self::Protected(o),
            (Self::Head(s), Self::Head(o)) => Self::Head(s.saturating_add(o)),
            (Self::Head(s), _) => Self::Head(s),
            (_, Self::Head(s)) => Self::Head(s),
            (Self::Commit(s), Self::Commit(o)) => Self::Commit(s.saturating_add(o)),
        }
    }
}

impl std::ops::Add<usize> for Weight {
    type Output = Self;

    fn add(self, other: usize) -> Self {
        match self {
            Self::Protected(s) => Self::Protected(s.saturating_add(other)),
            Self::Head(s) => Self::Head(s.saturating_add(other)),
            Self::Commit(s) => Self::Commit(s.saturating_add(other)),
        }
    }
}

impl std::ops::AddAssign for Weight {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other;
    }
}

impl std::ops::AddAssign<usize> for Weight {
    fn add_assign(&mut self, other: usize) {
        *self = *self + other;
    }
}

impl Default for Weight {
    fn default() -> Self {
        Self::Commit(1)
    }
}

fn format_commit(
    commit_id: git2::Oid,
    repo: &git_stack::git::GitRepo,
    graph: &git_stack::graph::Graph,
    head_branch: &git_stack::git::Branch,
    palette: &Palette,
) -> String {
    let action = graph
        .commit_get::<git_stack::graph::Action>(commit_id)
        .copied()
        .unwrap_or_default();
    let branches = graph.branches.get(commit_id).unwrap_or(&[]);
    let branch = if branches.is_empty() {
        let abbrev_id = repo
            .raw()
            .find_object(commit_id, None)
            .unwrap()
            .short_id()
            .unwrap();
        let style = if head_branch.id == commit_id {
            palette.highlight
        } else if action.is_protected() {
            palette.info
        } else if 1 < graph.children_of(commit_id).count() {
            // Branches should be off of other branches
            palette.warn
        } else if 1 < graph.parents_of(commit_id).count() {
            // Branches should be off of other branches
            palette.warn
        } else {
            palette.hint
        };
        format!("{}", style.paint(abbrev_id.as_str().unwrap()))
    } else {
        let mut branches = branches.to_owned();
        branches.sort_by_key(|b| {
            let is_head = head_branch == b.git();
            let head_first = !is_head;
            (
                head_first,
                b.remote().map(|r| r.to_owned()),
                b.base_name().to_owned(),
            )
        });
        branches
            .iter()
            .filter(|b| {
                if b.remote().is_some() {
                    let local_present = branches
                        .iter()
                        .any(|c| c.local_name() == Some(b.base_name()));
                    !local_present
                } else {
                    true
                }
            })
            .map(|b| {
                format!(
                    "{}{}",
                    format_branch_name(b, graph, head_branch, palette),
                    format_branch_status(b, repo, graph, palette),
                )
            })
            .join(", ")
    };

    let commit_status = format!("{} ", format_commit_status(commit_id, graph, palette));

    let commit = repo.find_commit(commit_id).expect("all commits present");
    let summary = String::from_utf8_lossy(&commit.summary);
    let description = if action.is_protected() {
        format!("{}", palette.hint.paint(summary))
    } else if commit.fixup_summary().is_some() {
        // Needs to be squashed
        format!("{}", palette.warn.paint(summary))
    } else if commit.wip_summary().is_some() {
        // Not for pushing implicitly
        format!("{}", palette.error.paint(summary))
    } else {
        format!("{}", summary)
    };

    [branch, commit_status, description].join("")
}

fn format_branch_name(
    branch: &git_stack::graph::Branch,
    graph: &git_stack::graph::Graph,
    head_branch: &git_stack::git::Branch,
    palette: &Palette,
) -> impl std::fmt::Display {
    if head_branch.id == branch.id() && head_branch == branch.git() {
        palette.highlight.paint(branch.name())
    } else {
        match branch.kind() {
            git_stack::graph::BranchKind::Deleted => palette.hint.paint(branch.name()),
            git_stack::graph::BranchKind::Mutable => {
                let action = graph
                    .commit_get::<git_stack::graph::Action>(branch.id())
                    .copied()
                    .unwrap_or_default();
                if action.is_protected() {
                    // Either haven't started dev or it got merged
                    palette.warn.paint(branch.name())
                } else {
                    palette.good.paint(branch.name())
                }
            }
            git_stack::graph::BranchKind::Mixed => {
                let action = graph
                    .commit_get::<git_stack::graph::Action>(branch.id())
                    .expect("all branches have nodes");
                if action.is_protected() {
                    palette.good.paint(branch.name())
                } else {
                    // Doing dev on a protected branch is awkward
                    palette.warn.paint(branch.name())
                }
            }
            git_stack::graph::BranchKind::Protected => palette.info.paint(branch.name()),
        }
    }
}

fn format_branch_status(
    branch: &git_stack::graph::Branch,
    repo: &git_stack::git::GitRepo,
    graph: &git_stack::graph::Graph,
    palette: &Palette,
) -> String {
    // See format_commit_status
    match branch.kind() {
        git_stack::graph::BranchKind::Deleted => String::new(),
        git_stack::graph::BranchKind::Mutable => {
            if 1 < graph.parents_of(branch.id()).count() {
                String::new()
            } else {
                match commit_relation(repo, branch.id(), branch.push_id()) {
                    Some((0, 0)) => {
                        format!(" {}", palette.good.paint("(pushed)"))
                    }
                    Some((local, 0)) => {
                        format!(" {}", palette.info.paint(format!("({} ahead)", local)))
                    }
                    Some((0, remote)) => {
                        format!(" {}", palette.warn.paint(format!("({} behind)", remote)))
                    }
                    Some((local, remote)) => {
                        format!(
                            " {}",
                            palette
                                .warn
                                .paint(format!("({} ahead, {} behind)", local, remote)),
                        )
                    }
                    None => {
                        let push_status = graph
                            .commit_get::<git_stack::graph::PushStatus>(branch.id())
                            .copied()
                            .unwrap_or_default();
                        if matches!(push_status, git_stack::graph::PushStatus::Pushable) {
                            format!(" {}", palette.info.paint("(ready)"))
                        } else {
                            String::new()
                        }
                    }
                }
            }
        }
        git_stack::graph::BranchKind::Mixed | git_stack::graph::BranchKind::Protected => {
            if branch.remote().is_none() && branch.pull_id().is_none() {
                format!(" {}", palette.warn.paint("(no remote)"))
            } else {
                String::new()
            }
        }
    }
}

fn format_commit_status(
    commit_id: git2::Oid,
    graph: &git_stack::graph::Graph,
    palette: &Palette,
) -> String {
    let action = graph
        .commit_get::<git_stack::graph::Action>(commit_id)
        .copied()
        .unwrap_or_default();

    // See format_branch_status
    if action.is_protected() {
        String::new()
    } else if action.is_delete() {
        format!(" {}", palette.error.paint("(drop)"))
    } else if 1 < graph.parents_of(commit_id).count() {
        format!(" {}", palette.error.paint("(merge commit)"))
    } else {
        String::new()
    }
}

fn commit_relation(
    repo: &git_stack::git::GitRepo,
    local: git2::Oid,
    remote: Option<git2::Oid>,
) -> Option<(usize, usize)> {
    let remote = remote?;
    if local == remote {
        return Some((0, 0));
    }

    let base = repo.merge_base(local, remote)?;
    let local_count = repo.commit_count(base, local)?;
    let remote_count = repo.commit_count(base, remote)?;
    Some((local_count, remote_count))
}

fn format_elide(elide_count: std::num::NonZeroUsize, palette: &Palette) -> String {
    format!(
        "{}",
        palette.hint.paint(format!("{elide_count} commits hidden"))
    )
}

#[derive(Copy, Clone, Debug)]
struct Palette {
    error: yansi::Style,
    warn: yansi::Style,
    info: yansi::Style,
    good: yansi::Style,
    highlight: yansi::Style,
    hint: yansi::Style,
}

impl Palette {
    pub fn colored() -> Self {
        Self {
            error: yansi::Style::new(yansi::Color::Red).bold(),
            warn: yansi::Style::new(yansi::Color::Yellow).bold(),
            info: yansi::Style::new(yansi::Color::Blue).bold(),
            good: yansi::Style::new(yansi::Color::Cyan).bold(),
            highlight: yansi::Style::new(yansi::Color::Green).bold(),
            hint: yansi::Style::new(yansi::Color::Unset).dimmed(),
        }
    }

    pub fn plain() -> Self {
        Self {
            error: yansi::Style::default(),
            warn: yansi::Style::default(),
            info: yansi::Style::default(),
            good: yansi::Style::default(),
            highlight: yansi::Style::default(),
            hint: yansi::Style::default(),
        }
    }
}
