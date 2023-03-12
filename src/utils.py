from dataclasses import fields


def deserialize_json(klass, d):
    values = {}
    for f in fields(klass):
        if f.name not in d:
            value = f.default
        else:
            value = d[f.name]
        values[f.name] = value
    return klass(**values)
