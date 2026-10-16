declare const document$: { subscribe(callback: () => void): void } | undefined;
declare const PYTHON_WORKER_URL: string;

(() => {
    const workerURL = new URL(
        PYTHON_WORKER_URL,
        (document.currentScript as HTMLScriptElement).src,
    );
    const active = new Set<() => void>();

    function enhance() {
        for (const dispose of active) dispose();
        active.clear();
        for (const example of document.querySelectorAll("[data-python-example]")) {
            const source = example.querySelector("pre code");
            if (!source || example.querySelector("form")) continue;

            const form = document.createElement("form");
            form.className = "python-example";
            const label = document.createElement("label");
            label.textContent = "Python · edit and run";
            const editor = document.createElement("textarea");
            editor.value = source.textContent;
            editor.name = "code";
            editor.rows = Math.min(18, editor.value.trimEnd().split("\n").length + 1);
            editor.spellcheck = false;
            editor.autocapitalize = "off";
            label.append(editor);
            const controls = document.createElement("div");
            controls.className = "python-example-controls";
            const run = document.createElement("button");
            run.type = "submit";
            run.textContent = "Run";
            const reset = document.createElement("button");
            reset.type = "button";
            reset.textContent = "Reset";
            const stop = document.createElement("button");
            stop.type = "button";
            stop.textContent = "Stop";
            stop.disabled = true;
            controls.append(run, reset, stop);
            const status = document.createElement("p");
            status.setAttribute("role", "status");
            status.textContent =
                "Runs locally in your browser. Python downloads on first run.";
            const output = document.createElement("pre");
            output.setAttribute("aria-label", "Python output");
            const initialOutput = [...example.querySelectorAll("pre")]
                .filter((block) => !block.contains(source))
                .map((block) => block.textContent)
                .join("\n");
            output.textContent = initialOutput;
            output.hidden = !initialOutput;
            const picture = document.createElement("iframe");
            picture.title = "Visual output from the Python example";
            picture.setAttribute("sandbox", "");
            picture.hidden = true;
            form.append(label, controls, status, output, picture);
            example.replaceChildren(form);

            let worker: Worker | undefined;
            const finish = (message: string) => {
                status.textContent = message;
                run.disabled = false;
                stop.disabled = true;
            };
            const dispose = () => {
                worker?.terminate();
                worker = undefined;
            };
            active.add(dispose);
            stop.onclick = () => {
                dispose();
                finish("Stopped. Run starts a fresh Python session.");
            };
            reset.onclick = () => {
                dispose();
                editor.value = source.textContent;
                output.textContent = initialOutput;
                output.hidden = !initialOutput;
                picture.hidden = true;
                picture.removeAttribute("srcdoc");
                finish("Example reset.");
            };
            form.onsubmit = (event) => {
                event.preventDefault();
                const code = editor.value;
                run.disabled = true;
                stop.disabled = false;
                output.textContent = "";
                output.hidden = true;
                picture.hidden = true;
                status.textContent = "Loading Python…";
                try {
                    if (!worker) {
                        worker = new Worker(workerURL, { type: "module" });
                        worker.onmessage = ({ data }) => {
                            switch (data.type) {
                                case "status":
                                    status.textContent = data.text;
                                    break;
                                case "output":
                                    output.hidden = false;
                                    output.textContent += `${data.text}\n`;
                                    break;
                                case "svg":
                                    picture.hidden = false;
                                    picture.srcdoc = data.text;
                                    break;
                                case "done":
                                    finish("Finished.");
                                    break;
                                case "error":
                                    output.hidden = false;
                                    output.textContent += `${data.text}\n`;
                                    dispose();
                                    finish(
                                        "Python reported an error. Edit the example and run again.",
                                    );
                                    break;
                            }
                        };
                        worker.onerror = () => {
                            dispose();
                            finish("Python could not start. Run to retry.");
                        };
                    }
                    worker.postMessage({ code });
                } catch (error) {
                    dispose();
                    finish(
                        `${error instanceof Error ? error.message : String(error)}. Run to retry.`,
                    );
                }
            };
        }
    }

    if (typeof document$ !== "undefined") document$.subscribe(enhance);
    else enhance();
})();
