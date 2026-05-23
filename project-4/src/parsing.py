import json


def parse_hierarchy(raw_json: str):
    tree = json.loads(raw_json)
    root = tree.get("activity", {}).get("root", tree)

    stack = [root]
    out = []

    while stack:
        node = stack.pop()

        if not isinstance(node, dict):
            continue

        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()

        if text or cls:
            out.append((
                cls.rsplit(".", 1)[-1] if cls else "",
                text,
                tuple(node.get("bounds") or [0, 0, 0, 0])
            ))

        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))

    return out


def text_representation(elements):
    filtered = [e for e in elements if e[1]]
    ordered = sorted(filtered, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(t for _, t, _ in ordered)