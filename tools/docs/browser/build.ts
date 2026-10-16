import { mkdir, unlink } from "node:fs/promises";
import { basename, dirname, join, relative, resolve } from "node:path";
import { finalizeAssets } from "./assets";

type Configuration = {
    site: string;
    siteURL: string;
    fonts?: boolean;
    stylesheets?: Record<string, string>;
    python?: {
        wheels: string[];
        packages: string[];
    };
};
type Package = {
    file_name: string;
    sha256: string;
    depends: string[];
};

const config: Configuration = JSON.parse(Bun.argv[2]);
const destination = resolve(config.site);
for (const name of config.python ? ["pyodide", "cpython"] : []) {
    await Bun.write(
        join(destination, "assets/licenses", `${name}.txt`),
        Bun.file(join(import.meta.dir, "licenses", `${name}.txt`)),
    );
}
for (const name of config.fonts ? ["inter", "jetbrains-mono"] : []) {
    await Bun.write(
        join(destination, "assets/licenses", `${name}.txt`),
        Bun.file(
            resolve(
                import.meta.dir,
                "../../../node_modules/@fontsource-variable",
                name,
                "LICENSE",
            ),
        ),
    );
}
if (config.python) {
    const root = resolve(import.meta.dir, "../../..");
    const manifest = await Bun.file(join(root, "package.json")).json();
    const packages = new Set(
        Object.keys(manifest.devDependencies).filter(
            (name) => name.startsWith("@codemirror/") || name.startsWith("@lezer/"),
        ),
    );
    const notices: string[] = [];
    for (const name of packages) {
        const directory = join(root, "node_modules", name);
        const metadata = await Bun.file(join(directory, "package.json")).json();
        notices.push(
            `${name} ${metadata.version}\n\n${await Bun.file(join(directory, "LICENSE")).text()}`,
        );
        for (const dependency of Object.keys(metadata.dependencies ?? {}))
            packages.add(dependency);
    }
    await Bun.write(
        join(destination, "assets/licenses/python-editor.txt"),
        notices.join("\n\n"),
    );
}

async function publish(outputs: Bun.BuildArtifact[]) {
    const renamed = new Map<string, string>();
    const ordered = [
        ...outputs.filter((output) => !/\.(js|css)$/.test(output.path)),
        ...outputs.filter((output) => /\.(js|css)$/.test(output.path)),
    ];
    for (const output of ordered) {
        let bytes = new Uint8Array(await output.arrayBuffer());
        if (/\.(js|css)$/.test(output.path)) {
            let text = new TextDecoder().decode(bytes);
            for (const [original, target] of renamed)
                text = text.replaceAll(original, target);
            bytes = new TextEncoder().encode(text);
        }
        const digest = new Bun.CryptoHasher("sha256")
            .update(bytes)
            .digest("hex")
            .slice(0, 16);
        const name = basename(output.path).replace(/\.[^.]+(\.[^.]+)$/, `.${digest}$1`);
        await Bun.write(join(dirname(output.path), name), bytes);
        if (basename(output.path) !== name) await unlink(output.path);
        renamed.set(basename(output.path), name);
    }
    return renamed;
}
async function stagePython(config: NonNullable<Configuration["python"]>) {
    const pyodideRoot = resolve(import.meta.dir, "../../../node_modules/pyodide");
    const { version } = await Bun.file(join(pyodideRoot, "package.json")).json();
    const lock: { packages: Record<string, Package> } = await Bun.file(
        join(pyodideRoot, "pyodide-lock.json"),
    ).json();

    const runtimeFiles = [
        "pyodide.mjs",
        "pyodide.asm.mjs",
        "pyodide.asm.wasm",
        "python_stdlib.zip",
        "pyodide-lock.json",
    ];
    const runtimeHash = new Bun.CryptoHasher("sha256");
    for (const file of runtimeFiles) {
        runtimeHash
            .update(file)
            .update(await Bun.file(join(pyodideRoot, file)).arrayBuffer());
    }
    const runtimePath = `assets/python/${runtimeHash.digest("hex")}`;
    const runtimeDirectory = join(destination, runtimePath);
    await mkdir(runtimeDirectory, { recursive: true });
    for (const file of runtimeFiles) {
        await Bun.write(
            join(runtimeDirectory, file),
            Bun.file(join(pyodideRoot, file)),
        );
    }

    const selected = new Set<string>();
    function include(name: string) {
        if (selected.has(name)) return;
        const entry = lock.packages[name];
        if (!entry) throw new Error(`Unknown Pyodide package: ${name}`);
        selected.add(name);
        for (const dependency of entry.depends) include(dependency);
    }
    for (const name of config.packages) include(name);

    const cache = resolve(
        import.meta.dir,
        "../../../node_modules/.cache/pyodide",
        version,
    );
    await mkdir(cache, { recursive: true });
    const packages = await Promise.all(
        [...selected].map(async (name) => {
            const entry = lock.packages[name];
            const cached = Bun.file(join(cache, entry.file_name));
            let bytes: ArrayBuffer;
            if (await cached.exists()) {
                bytes = await cached.arrayBuffer();
            } else {
                const response = await fetch(
                    `https://cdn.jsdelivr.net/pyodide/v${version}/full/${entry.file_name}`,
                );
                if (!response.ok)
                    throw new Error(`Downloading ${name}: HTTP ${response.status}`);
                bytes = await response.arrayBuffer();
            }
            const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
            if (digest !== entry.sha256)
                throw new Error(`Invalid Pyodide package digest: ${name}`);
            await Bun.write(cached, bytes);
            const path = `assets/python/${digest}/${entry.file_name}`;
            await Bun.write(join(destination, path), bytes);
            return `../python/${digest}/${entry.file_name}`;
        }),
    );

    for (const wheel of config.wheels) {
        const bytes = await Bun.file(wheel).arrayBuffer();
        const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
        await Bun.write(
            join(destination, `assets/python/${digest}/${basename(wheel)}`),
            bytes,
        );
        packages.push(`../python/${digest}/${basename(wheel)}`);
    }
    return { runtime: `../python/${basename(runtimePath)}/`, packages };
}

const sharedDirectory = join(destination, "assets/shared");
const naming = { entry: "[name].[hash].[ext]", asset: "[name].[hash].[ext]" };
let workerPath: string | undefined;
if (config.python) {
    const python = await stagePython(config.python);
    const worker = await Bun.build({
        entrypoints: [join(import.meta.dir, "python-worker.ts")],
        outdir: sharedDirectory,
        target: "browser",
        minify: true,
        naming,
        define: {
            PYTHON_RUNTIME_URL: JSON.stringify(python.runtime),
            PYTHON_PACKAGE_URLS: JSON.stringify(python.packages),
        },
        plugins: [
            {
                name: "self-hosted-python",
                setup(build) {
                    build.onResolve({ filter: /^pyodide$/ }, () => ({
                        path: `${python.runtime}pyodide.mjs`,
                        external: true,
                    }));
                },
            },
        ],
    });
    if (!worker.success)
        throw new AggregateError(worker.logs, "Python worker build failed");
    const published = await publish(worker.outputs);
    workerPath = published.get(basename(worker.outputs[0].path));
}

const result = await Bun.build({
    loader: { ".woff2": "file" },
    entrypoints: [
        join(import.meta.dir, "accessibility.ts"),
        join(import.meta.dir, "common.css"),
        ...(config.fonts ? [join(import.meta.dir, "fonts.css")] : []),
        ...Object.values(config.stylesheets ?? {}),
        ...(config.python
            ? [
                  join(import.meta.dir, "python-examples.ts"),
                  join(import.meta.dir, "python-examples.css"),
              ]
            : []),
    ],
    outdir: sharedDirectory,
    target: "browser",
    minify: true,
    naming,
    define: { PYTHON_WORKER_URL: JSON.stringify(workerPath ?? "") },
});
if (!result.success)
    throw new AggregateError(result.logs, "Browser asset build failed");

const mapping: Record<string, string> = {};
const published = await publish(result.outputs);
for (const output of result.outputs.filter(
    (output) => output.kind === "entry-point" || output.loader === "css",
)) {
    const name = basename(output.path).replace(/\.[^.]+\.(js|css)$/, ".$1");
    const authored = Object.entries(config.stylesheets ?? {}).find(
        ([, path]) => basename(path) === name,
    );
    mapping[authored?.[0] ?? `assets/shared/${name}`] = relative(
        destination,
        join(dirname(output.path), published.get(basename(output.path))!),
    );
}
await finalizeAssets(
    destination,
    config.siteURL,
    mapping,
    Object.keys(config.stylesheets ?? {}),
);
