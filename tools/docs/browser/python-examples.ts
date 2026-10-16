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
    let worker: Worker | undefined;
    let running:
        | {
              element: Element;
              finish: (message: string) => void;
              receive: (data: { type: string; text: string }) => void;
          }
        | undefined;
    const stopExecution = (message = "Stopped.") => {
        worker?.terminate();
        worker = undefined;
        running?.finish(message);
    };
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
        const headings = [
            ...document.querySelectorAll("article :is(h1, h2, h3, h4, h5, h6)[id]"),
        ];
        for (const example of document.querySelectorAll("[data-python-example]")) {
            const source = example.querySelector("pre code");
            if (!source || example.querySelector("form")) continue;

            const form = document.createElement("form");
            form.className = "python-example";
            const heading = headings.findLast(
                (candidate) =>
                    candidate.compareDocumentPosition(example) &
                    Node.DOCUMENT_POSITION_FOLLOWING,
            );
            if (heading) form.setAttribute("aria-labelledby", heading.id);
            else form.setAttribute("aria-label", "Python example");
            const originalCode = (
                example.getAttribute("data-python-code") ?? source.textContent
            ).trimEnd();
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
            status.textContent = example.hasAttribute("data-python-intro")
                ? "Runs locally in your browser. Python downloads on first run."
                : "";
            status.hidden = !status.textContent;
            const toolbar = document.createElement("div");
            toolbar.className = "python-example-toolbar";
            toolbar.append(controls, status);
            const output = document.createElement("pre");
            output.setAttribute("aria-label", "Python output");
            const initialOutput = (
                example.getAttribute("data-python-output") ??
                [...example.querySelectorAll("pre")]
                    .filter((block) => !block.contains(source))
                    .map((block) => block.textContent)
                    .join("\n")
            ).trimEnd();
            output.textContent = initialOutput;
            output.hidden = !initialOutput;
            let picture = document.createElement("img");
            picture.alt = "Visual output from the Python example";
            picture.hidden = true;
            const results = document.createElement("div");
            results.className = "python-example-results";
            results.append(output, picture);
            form.append(toolbar, results);
            example.replaceChildren(form);

            const finish = (message: string) => {
                status.textContent = message;
                status.hidden = !message;
                running = undefined;
                run.textContent = "Run";
                if (
                    !output.classList.contains("python-example-stale") ||
                    !picture.classList.contains("python-example-stale")
                ) {
                    if (output.classList.contains("python-example-stale"))
                        output.hidden = true;
                    if (picture.classList.contains("python-example-stale"))
                        picture.hidden = true;
                }
                results.removeAttribute("aria-busy");
                results.style.minHeight = "";
                output.classList.remove("python-example-stale");
                picture.classList.remove("python-example-stale");
            };
            active.set(example, () => {
                if (running?.element === example) stopExecution();
                editor.destroy();
            });
            const appendOutput = (text: string) => {
                output.textContent += `${output.hidden ? "" : "\n"}${text}`;
                output.hidden = false;
            };
            reset.onclick = () => {
                if (running?.element === example) stopExecution();
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
                picture.removeAttribute("src");
                status.textContent = "Example reset.";
                status.hidden = false;
            };
            form.onsubmit = (event) => {
                event.preventDefault();
                if (running?.element === example) {
                    stopExecution();
                    return;
                }
                if (running) stopExecution("Stopped to run another example.");
                const code = editor.state.doc.toString();
                let hasOutput = false;
                let rendering: Promise<boolean> | undefined;
                const execution = {
                    element: example,
                    finish,
                    receive: (data: { type: string; text: string }) => {
                        switch (data.type) {
                            case "status":
                                status.textContent = data.text;
                                break;
                            case "output":
                                if (!hasOutput) {
                                    output.textContent = "";
                                    output.hidden = true;
                                    output.classList.remove("python-example-stale");
                                    hasOutput = true;
                                }
                                appendOutput(data.text);
                                break;
                            case "svg":
                                {
                                    const next = new Image();
                                    next.alt = picture.alt;
                                    next.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(data.text)}`;
                                    rendering = next.decode().then(
                                        () => {
                                            if (running === execution) {
                                                picture.replaceWith(next);
                                                picture = next;
                                            }
                                            return true;
                                        },
                                        () => false,
                                    );
                                }
                                break;
                            case "done":
                                void Promise.resolve(rendering).then((decoded) => {
                                    if (running !== execution) return;
                                    if (decoded === false) {
                                        picture.hidden = true;
                                        finish(
                                            "The visual output could not be displayed. Run to retry.",
                                        );
                                        return;
                                    }
                                    if (!hasOutput) {
                                        output.textContent = "";
                                        output.hidden = true;
                                    }
                                    if (!rendering) picture.hidden = true;
                                    finish("Finished.");
                                });
                                break;
                            case "error":
                                if (!hasOutput) {
                                    output.textContent = "";
                                    output.hidden = true;
                                }
                                picture.hidden = true;
                                appendOutput(data.text.trimEnd());
                                stopExecution(
                                    "Python reported an error. Edit the example and run again.",
                                );
                                break;
                        }
                    },
                };
                running = execution;
                run.textContent = "Stop";
                results.style.minHeight = `${results.getBoundingClientRect().height}px`;
                results.setAttribute("aria-busy", "true");
                output.classList.add("python-example-stale");
                picture.classList.add("python-example-stale");
                status.textContent = "Loading Python…";
                status.hidden = false;
                try {
                    if (!worker) {
                        const created = new Worker(workerURL, { type: "module" });
                        worker = created;
                        created.onmessage = ({ data }) => {
                            if (worker === created) running?.receive(data);
                        };
                        created.onerror = () => {
                            if (worker === created)
                                stopExecution("Python could not start. Run to retry.");
                        };
                    }
                    worker.postMessage({ code });
                } catch (error) {
                    stopExecution(
                        `${error instanceof Error ? error.message : String(error)}. Run to retry.`,
                    );
                }
            };
        }
    }

    if (typeof document$ !== "undefined") document$.subscribe(enhance);
    else enhance();
})();
