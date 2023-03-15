import itertools
from pathlib import Path
from typing import IO

FileOrPath = Path | str | IO


Tree = dict[str, "Tree"] | None


def create_path_tree(paths: list[Path]) -> Tree:
    out_dict = {}
    if len(paths) == 0:
        return None

    for key, group in itertools.groupby(paths, lambda x: x.parts[0]):
        listed_group = list(group)
        path_remainders = [Path(*p.parts[1:]) for p in listed_group if len(p.parts) > 1]
        out_dict[str(key)] = create_path_tree(path_remainders)
    return out_dict
