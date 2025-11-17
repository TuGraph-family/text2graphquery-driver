import json

def schema_to_text(schema_json):
    lines, vertices, edges = [], [], []
    for item in schema_json["schema"]:
        label = item["label"]
        type_ = item["type"]
        props = item.get("properties", [])
        props_str = ", ".join(
            [f'{p["name"]}: {p["type"]}' + (" (optional)" if p.get("optional") else "")
             for p in props]
        )

        if type_ == "VERTEX":
            primary = item.get("primary")
            if primary:
                vertices.append(f"- {label} [primary: {primary}] ({props_str})")
            else:
                vertices.append(f"- {label}({props_str})")
        elif type_ == "EDGE":
            temporal = item.get("temporal")
            if temporal:
                edges.append(f"- {label} [temporal: {temporal}] ({props_str})")
            else:
                edges.append(f"- {label}({props_str})")

    if vertices:
        lines.append("Vertex types:")
        lines.extend(vertices)
    if edges:
        lines.append("\nEdge types:")
        lines.extend(edges)

    return "\n".join(lines)
