from __future__ import annotations

from pydantic import DirectoryPath, Field

from checker.plugins import PluginABC, PluginOutput
from checker.plugins.scripts import RunScriptPlugin


class RunPytestPlugin(RunScriptPlugin):
    """Plugin for running pytest."""

    name = "run_pytest"

    class Args(PluginABC.Args):
        origin: str
        target: str
        timeout: int | None = None
        isolate: bool = False
        env_whitelist: list[str] = Field(default_factory=lambda: ['PATH'])

        coverage: bool | int | None = None

    def _run(self, args: Args, *, verbose: bool = False) -> PluginOutput:
        tests_cmd = ['python', '-m', 'pytest']

        if not verbose:
            tests_cmd += ['--no-header']
            tests_cmd += ['--tb=no']

        if args.coverage:
            tests_cmd += ['--cov-report', 'term-missing']
            tests_cmd += ['--cov', args.target]
            if args.coverage is not True:
                tests_cmd += ['--cov-fail-under', str(args.coverage)]
        else:
            tests_cmd += ['-p', 'no:cov']

        env_whitelist=args.env_whitelist
        env_whitelist.append("YT_PROXY")
        env_whitelist.append("YT_TOKEN")

        run_script_args = RunScriptPlugin.Args(
            origin=args.origin,
            script=' '.join(tests_cmd + [args.target]),  # TODO: check, not working when list
            timeout=args.timeout,
            isolate=args.isolate,
            env_whitelist=env_whitelist,
        )
        result = super()._run(run_script_args, verbose=verbose)
        return result
