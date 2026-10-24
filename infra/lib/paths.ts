import * as fs from "fs";
import * as path from "path";

/**
 * The repository root, found from this file whether it runs as TypeScript
 * (infra/lib) or compiled (infra/dist/lib), so asset paths don't depend on
 * the working directory.
 */
export const REPO_ROOT: string = (() => {
  let dir = __dirname;
  for (let i = 0; i < 6; i++) {
    if (fs.existsSync(path.join(dir, "infra", "cdk.json")) && fs.existsSync(path.join(dir, "lambda"))) return dir;
    dir = path.dirname(dir);
  }
  throw new Error(`Could not find the repository root above ${__dirname}`);
})();

/** A path inside the repository. */
export function repoPath(...parts: string[]): string {
  return path.join(REPO_ROOT, ...parts);
}
