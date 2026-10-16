import { loadPyodide, type PyodideInterface } from "pyodide";
import type { PyProxy } from "pyodide/ffi";

declare const PYTHON_RUNTIME_URL: string;
declare const PYTHON_PACKAGE_URLS: string[];
type Request = { code: string };
let runtime: PyodideInterface | undefined;

self.onmessage = async ({ data: { code } }: MessageEvent<Request>) => {
    let globals: PyProxy | undefined;
    let result: unknown;
    try {
        if (!runtime) {
            self.postMessage({ type: "status", text: "Loading Python packages…" });
            const loaded = await loadPyodide({
                indexURL: new URL(PYTHON_RUNTIME_URL, import.meta.url).href,
            });
            await loaded.loadPackage(
                PYTHON_PACKAGE_URLS.map((path) => new URL(path, import.meta.url).href),
            );
            runtime = loaded;
        }
        runtime.setStdout({
            batched: (text) => self.postMessage({ type: "output", text }),
        });
        runtime.setStderr({
            batched: (text) => self.postMessage({ type: "output", text }),
        });
        globals = runtime.toPy({ __name__: "__main__" });
        self.postMessage({ type: "status", text: "Running…" });
        result = await runtime.runPythonAsync(code, { globals });
        if (result != null) {
            if (
                result instanceof runtime.ffi.PyProxy &&
                typeof result._repr_svg_ === "function"
            ) {
                self.postMessage({ type: "svg", text: result._repr_svg_() });
            } else {
                self.postMessage({ type: "output", text: String(result) });
            }
        }
        self.postMessage({ type: "done" });
    } catch (error) {
        self.postMessage({ type: "error", text: String(error) });
    } finally {
        if (runtime && result instanceof runtime.ffi.PyProxy) result.destroy();
        globals?.destroy();
    }
};
