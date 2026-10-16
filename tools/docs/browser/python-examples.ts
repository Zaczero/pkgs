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
    const active = new Map<Element, { code: () => string; dispose: () => void }>();
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
        for (const [example, state] of active) {
            if (!example.isConnected) {
                state.dispose();
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
            const picture = document.createElement("iframe");
            picture.title = "Visual output from the Python example";
            picture.setAttribute("sandbox", "");
            picture.hidden = true;
            form.append(toolbar, output, picture);
            example.replaceChildren(form);

            const finish = (message: string) => {
                status.textContent = message;
                status.hidden = !message;
                running = undefined;
                run.textContent = "Run";
            };
            active.set(example, {
                code: () => editor.state.doc.toString(),
                dispose: () => {
                    if (running?.element === example) stopExecution();
                    editor.destroy();
                },
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
                picture.removeAttribute("srcdoc");
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
                const context: string[] = [];
                const session = example.getAttribute("data-python-session");
                if (session) {
                    for (const [previous, state] of active) {
                        if (previous === example) break;
                        if (previous.getAttribute("data-python-session") === session)
                            context.push(state.code());
                    }
                }
                running = {
                    element: example,
                    finish,
                    receive: (data) => {
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
                                stopExecution(
                                    "Python reported an error. Edit the example and run again.",
                                );
                                break;
                        }
                    },
                };
                run.textContent = "Stop";
                output.textContent = "";
                output.hidden = true;
                picture.hidden = true;
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
                    worker.postMessage({ code, context });
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
