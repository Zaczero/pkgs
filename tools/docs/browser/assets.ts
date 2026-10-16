import { rename, unlink } from "node:fs/promises";
import { basename, dirname, join, posix } from "node:path";

export async function finalizeAssets(
    site: string,
    siteURL: string,
    mapping: Record<string, string>,
    stylesheets: string[],
) {
    for await (const path of new Bun.Glob("assets/**/*").scan({ cwd: site })) {
        if (
            path.startsWith("assets/shared/") ||
            path.startsWith("assets/python/") ||
            !/\.(svg|ico|png|jpg|webp|avif|js|css)$/.test(path) ||
            /\.[0-9a-f]{6,64}(?:\.min)?\./.test(basename(path))
        )
            continue;
        const bytes = await Bun.file(join(site, path)).arrayBuffer();
        const digest = new Bun.CryptoHasher("sha256")
            .update(bytes)
            .digest("hex")
            .slice(0, 16);
        const target = path.replace(/(\.[^.]+)$/, `.${digest}$1`);
        mapping[path] = target;
        await rename(join(site, path), join(site, target));
    }
    for (const path of stylesheets) {
        if (await Bun.file(join(site, path)).exists()) await unlink(join(site, path));
    }

    const canonical = new URL(siteURL);
    function localPath(value: string, page: string) {
        const url = new URL(value, new URL(page, canonical));
        if (
            url.origin !== canonical.origin ||
            !url.pathname.startsWith(canonical.pathname)
        )
            return;
        return decodeURIComponent(url.pathname.slice(canonical.pathname.length));
    }
    function rewrite(value: string, page: string) {
        const path = localPath(value, page);
        if (!path || !mapping[path]) return value;
        const url = new URL(value, new URL(page, canonical));
        const target = canonical.pathname + mapping[path];
        if (/^(?:https?:)?\/\//.test(value))
            return `${url.origin}${target}${url.search}${url.hash}`;
        return (
            (value.startsWith("/")
                ? target
                : posix.relative(dirname(page), mapping[path])) +
            url.search +
            url.hash
        );
    }

    for await (const page of new Bun.Glob("**/*.html").scan({ cwd: site })) {
        const html = new HTMLRewriter()
            .on("*", {
                async element(element) {
                    for (const attribute of ["href", "src", "poster"]) {
                        const value = element.getAttribute(attribute);
                        if (!value) continue;
                        const changed = rewrite(value, page);
                        element.setAttribute(attribute, changed);
                        const isAsset =
                            attribute !== "href" ||
                            (element.tagName === "link" &&
                                /\b(stylesheet|icon)\b/.test(
                                    element.getAttribute("rel") ?? "",
                                ));
                        const path = isAsset ? localPath(changed, page) : undefined;
                        if (path && !(await Bun.file(join(site, path)).exists())) {
                            throw new Error(`${page}: missing asset ${path}`);
                        }
                    }
                },
            })
            .transform(new Response(Bun.file(join(site, page))));
        await Bun.write(join(site, page), await html.text());
    }
    for await (const page of new Bun.Glob("**/*.md").scan({ cwd: site })) {
        const path = join(site, page);
        const text = await Bun.file(path).text();
        await Bun.write(
            path,
            text.replace(/(?<=\]\()([^\s)]+)/g, (url) => rewrite(url, page)),
        );
    }
}
