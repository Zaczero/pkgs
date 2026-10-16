import { loadPyodide, type PyodideInterface } from "pyodide";
import type { PyProxy } from "pyodide/ffi";

declare const PYTHON_RUNTIME_URL: string;
declare const PYTHON_PACKAGE_URLS: string[];
declare const PYTHON_IMPORT_HINTS: Record<string, string>;
type Request = { code: string; context: string[] };
let runtime: PyodideInterface | undefined;

self.onmessage = async ({ data: { code, context } }: MessageEvent<Request>) => {
    let globals: PyProxy | undefined;
    let evaluation: PyProxy | undefined;
    let result: unknown;
    try {
        if (!runtime) {
            self.postMessage({ type: "status", text: "Loading Python packages…" });
            const loaded = await loadPyodide({
                indexURL: new URL(PYTHON_RUNTIME_URL, import.meta.url).href,
            });
            await loaded.loadPackage(
                PYTHON_PACKAGE_URLS.map((path) =>
                    path.startsWith(".") ? new URL(path, import.meta.url).href : path,
                ),
            );
            runtime = loaded;
        }
        runtime.setStdout({ batched: () => {} });
        runtime.setStderr({ batched: () => {} });
        const allCode = [...context, code].join("\n\n");
        const packages = Object.entries(PYTHON_IMPORT_HINTS)
            .filter(([, pattern]) => new RegExp(pattern).test(allCode))
            .map(([name]) => name);
        if (packages.length)
            await runtime.loadPackage(packages, { messageCallback: () => {} });
        await runtime.loadPackagesFromImports(allCode, { messageCallback: () => {} });
        globals = runtime.toPy({ __name__: "__main__" });
        for (const [index, setup] of context.entries()) {
            self.postMessage({
                type: "status",
                text: `Preparing earlier example ${index + 1}…`,
            });
            try {
                const value = await runtime.runPythonAsync(setup, { globals });
                if (value instanceof runtime.ffi.PyProxy) value.destroy();
            } catch (error) {
                throw new Error(
                    `Earlier example ${index + 1} failed: ${String(error)}`,
                );
            }
        }
        runtime.setStdout({
            batched: (text) => self.postMessage({ type: "output", text }),
        });
        runtime.setStderr({
            batched: (text) => self.postMessage({ type: "output", text }),
        });
        self.postMessage({ type: "status", text: "Running…" });
        evaluation = runtime.toPy({ source: code, namespace: globals });
        result = await runtime.runPythonAsync(
            `
import ast
import inspect
import sys

svg = None
def display(value):
    global svg
    if value is None:
        return
    render = getattr(value, "_repr_svg_", None)
    if callable(render):
        svg = render()
    else:
        print(repr(value))

previous_display = sys.displayhook
try:
    sys.displayhook = display
    statements = ast.Interactive(body=ast.parse(source).body)
    executable = compile(
        statements, "<example>", "single",
        flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
    )
    pending = eval(executable, namespace)
    if inspect.isawaitable(pending):
        await pending
finally:
    sys.displayhook = previous_display
svg
`,
            { globals: evaluation },
        );
        if (result != null) {
            self.postMessage({ type: "svg", text: String(result) });
        }
        self.postMessage({ type: "done" });
    } catch (error) {
        self.postMessage({ type: "error", text: String(error) });
    } finally {
        if (runtime && result instanceof runtime.ffi.PyProxy) result.destroy();
        evaluation?.destroy();
        globals?.destroy();
    }
};
