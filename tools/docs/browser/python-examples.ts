import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { python } from "@codemirror/lang-python";
import { HighlightStyle, indentUnit, syntaxHighlighting } from "@codemirror/language";
import { EditorView, keymap } from "@codemirror/view";
import { tags } from "@lezer/highlight";

declare const PYTHON_WORKER_URL: string;

(() => {
    const workerURL = new URL(
        PYTHON_WORKER_URL,
        (document.currentScript as HTMLScriptElement).src,
    );
    const active = new Map<Element, () => void>();
    const highlighting = HighlightStyle.define([
        { tag: tags.keyword, color: "var(--md-code-hl-keyword-color)" },
        { tag: tags.comment, color: "var(--md-code-hl-comment-color)" },
        { tag: tags.string, color: "var(--md-code-hl-string-color)" },
        { tag: tags.number, color: "var(--md-code-hl-number-color)" },
        { tag: [tags.bool, tags.null], color: "var(--md-code-hl-constant-color)" },
        { tag: tags.operator, color: "var(--md-code-hl-operator-color)" },
        {
            tag: tags.function(tags.variableName),
            color: "var(--md-code-hl-function-color)",
        },
        {
            tag: tags.standard(tags.variableName),
            color: "var(--md-code-hl-name-color)",
        },
    ]);

    function enhance() {
        for (const [example, dispose] of active) {
            if (!example.isConnected) {
                dispose();
                active.delete(example);
            }
        }
        for (const example of document.querySelectorAll("[data-python-example]")) {
            const source = example.querySelector("pre code");
            if (!source || example.querySelector("form")) continue;

            const form = document.createElement("form");
            form.className = "python-example";
            const originalCode = source.textContent.trimEnd();
            const controls = document.createElement("div");
            controls.className = "python-example-controls";
            const run = document.createElement("button");
            run.type = "submit";
            run.textContent = "Run";
            const reset = document.createElement("button");
            reset.type = "button";
            reset.textContent = "Reset";
            reset.disabled = true;
            const editor = new EditorView({
                doc: originalCode,
                extensions: [
                    python(),
                    indentUnit.of("    "),
                    history(),
                    keymap.of([...defaultKeymap, ...historyKeymap]),
                    syntaxHighlighting(highlighting),
                    EditorView.lineWrapping,
                    EditorView.contentAttributes.of({
                        "aria-label": "Python code",
                        autocapitalize: "off",
                        spellcheck: "false",
                    }),
                    EditorView.updateListener.of((update) => {
                        if (update.docChanged) {
                            reset.disabled =
                                update.state.doc.toString() === originalCode;
                        }
                    }),
                ],
                parent: form,
            });
            controls.append(run, reset);
            const status = document.createElement("p");
            status.setAttribute("role", "status");
            status.textContent =
                "Runs locally in your browser. Python downloads on first run.";
            const output = document.createElement("pre");
            output.setAttribute("aria-label", "Python output");
            const initialOutput = [...example.querySelectorAll("pre")]
                .filter((block) => !block.contains(source))
                .map((block) => block.textContent)
                .join("\n")
                .trimEnd();
            output.textContent = initialOutput;
            output.hidden = !initialOutput;
            const picture = document.createElement("iframe");
            picture.title = "Visual output from the Python example";
            picture.setAttribute("sandbox", "");
            picture.hidden = true;
            form.append(controls, status, output, picture);
            example.replaceChildren(form);

            let worker: Worker | undefined;
            let running = false;
            const finish = (message: string) => {
                status.textContent = message;
                running = false;
                run.textContent = "Run";
            };
            const stopWorker = () => {
                worker?.terminate();
                worker = undefined;
            };
            active.set(example, () => {
                stopWorker();
                editor.destroy();
            });
            const appendOutput = (text: string) => {
                output.textContent += `${output.hidden ? "" : "\n"}${text}`;
                output.hidden = false;
            };
            reset.onclick = () => {
                stopWorker();
                editor.dispatch({
                    changes: {
                        from: 0,
                        to: editor.state.doc.length,
                        insert: originalCode,
                    },
                    selection: { anchor: 0 },
                });
                output.textContent = initialOutput;
                output.hidden = !initialOutput;
                picture.hidden = true;
                picture.removeAttribute("srcdoc");
                finish("Example reset.");
            };
            form.onsubmit = (event) => {
                event.preventDefault();
                if (running) {
                    stopWorker();
                    finish("Stopped.");
                    return;
                }
                const code = editor.state.doc.toString();
                running = true;
                run.textContent = "Stop";
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
                                    appendOutput(data.text);
                                    break;
                                case "svg":
                                    picture.hidden = false;
                                    picture.srcdoc = data.text;
                                    break;
                                case "done":
                                    finish("Finished.");
                                    break;
                                case "error":
                                    appendOutput(data.text.trimEnd());
                                    stopWorker();
                                    finish(
                                        "Python reported an error. Edit the example and run again.",
                                    );
                                    break;
                            }
                        };
                        worker.onerror = () => {
                            stopWorker();
                            finish("Python could not start. Run to retry.");
                        };
                    }
                    worker.postMessage({ code });
                } catch (error) {
                    stopWorker();
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
