import { mkdir, rename, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const repository = resolve(root, "..");
const output = join(root, "dist/wasm");
const { version } = await Bun.file(
    join(repository, "node_modules/pyodide/package.json"),
).json();
const identity = Bun.spawn(
    [
        join(root, ".venv/bin/python"),
        "-c",
        "import json,sys;from pathlib import Path;sys.path.insert(0,'tools');from artifact_content import source_input_digests;print(json.dumps(source_input_digests(Path.cwd()),sort_keys=True))",
    ],
    { cwd: root, stdout: "pipe", stderr: "inherit" },
);
const source = await new Response(identity.stdout).text();
if (await identity.exited)
    throw new Error("Could not identify WebAssembly source inputs");
const hash = new Bun.CryptoHasher("sha256").update(source).update(version);
const compiler = Bun.spawnSync(["rustc", "-vV"], {
    cwd: repository,
    stderr: "inherit",
});
if (compiler.exitCode) throw new Error("Could not identify WebAssembly compiler");
hash.update(compiler.stdout);
for (const file of [
    import.meta.path,
    join(root, "Cargo.toml"),
    join(root, "pyproject.toml"),
    join(repository, "Cargo.toml"),
    join(repository, "Cargo.lock"),
    join(repository, "rust-toolchain.toml"),
])
    hash.update(await Bun.file(file).arrayBuffer());
const fingerprint = hash.digest("hex");
const receipt = Bun.file(join(output, "build.json"));
if (await receipt.exists()) {
    const previous = await receipt.json();
    const wheel = Bun.file(join(output, previous.wheel));
    if (
        previous.source === fingerprint &&
        (await wheel.exists()) &&
        new Bun.CryptoHasher("sha256")
            .update(await wheel.arrayBuffer())
            .digest("hex") === previous.sha256
    ) {
        process.exit(0);
    }
}

const staging = join(root, "dist/wasm-build");
await rm(staging, { recursive: true, force: true });
await mkdir(staging, { recursive: true });
const tool = [
    "uv",
    "tool",
    "run",
    "--python",
    "3.14",
    "--from",
    "pyodide-cli==0.5.0",
    "--with",
    "pyodide-build==0.39.1",
    "--with",
    "maturin==1.15.0",
    "pyodide",
];
for (const arguments_ of [
    ["xbuildenv", "install", version],
    [
        "build",
        root,
        "--no-isolation",
        "-C",
        "build-args=--profile release",
        "--outdir",
        staging,
    ],
]) {
    const child = Bun.spawn([...tool, ...arguments_], {
        cwd: root,
        env: {
            ...process.env,
            CARGO_UNSTABLE_BUILD_STD: "std,panic_abort",
            CARGO_TARGET_DIR: join(repository, "target-pyodide"),
            EM_FROZEN_CACHE: "1",
        },
        stdout: "inherit",
        stderr: "inherit",
    });
    if (await child.exited)
        throw new Error(`WebAssembly build failed: ${arguments_[0]}`);
}
const wheels = [...new Bun.Glob("*.whl").scanSync({ cwd: staging })];
if (wheels.length !== 1)
    throw new Error("The WebAssembly build must produce one wheel");
const wheel = wheels[0];
await Bun.write(
    join(staging, "build.json"),
    JSON.stringify({
        source: fingerprint,
        wheel,
        sha256: new Bun.CryptoHasher("sha256")
            .update(await Bun.file(join(staging, wheel)).arrayBuffer())
            .digest("hex"),
    }),
);
await rm(output, { recursive: true, force: true });
await rename(staging, output);
