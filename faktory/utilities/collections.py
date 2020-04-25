from collections.abc import MutableMapping
from typing import Any, Dict, Iterable, Iterator, Optional, Union, cast

DictLike = Union[Dict, "DotDict"]


class CompoundKey(tuple):
    pass


class DotDict(MutableMapping):
    """
    A `dict` type object that also supports attribute ("dot") access. This
    should be considered an extension to a standard `dict`, and is used
    in place of a third party library (like `Box`) to avoid dependencies.

    Only items that you'd expect to be able to access on a class
    (valid identifiers) will be accessible via dot notation. This
    means strings that start with numbers, special characters,
    and double underscores will not be accessible via dot notation.

    Args:
        - init_dict (Optional[Dict]): Dictionary to initialize the
            `DotDict` with. If `None`, initializes an empty container.
        - **kwargs (Optional[Any]): Key value pairs to initialize the
            `DotDict`.

    Example:
        ```python
        dd = DotDict({"a": 34}, b=56, c=set())
        dd.a # 34
        dd['b'] # 56
        dd.c # set()
        ```
    """

    def __init__(self, init_dict: Optional[DictLike] = None, **kwargs: Any):
        # a DotDict could have a key that is the same as "update"
        if init_dict:
            super().update(init_dict)
        super().update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        """
        This method is specifically designed with MyPy integration in mind,
        as it complains that the `.get` is inherited incorrectly.

        Args:
            - key (str): The key whose value we want to retrieve
            - default (Any): A default value to return if not found

        Returns:
            - Any: Value of they key if found, otherwise default
        """

        return super().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.__dict__[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.__dict__[key] = value

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    def __iter__(self) -> Iterator[str]:
        return iter(self.__dict__.keys())

    def __delitem__(self, key: str) -> None:
        del self.__dict__[key]

    def __len__(self) -> int:
        return len(self.__dict__)

    def __repr__(self) -> str:
        if len(self) == 0:
            return "<{}>".format(type(self).__name__)

        return "<{}: {}>".format(
            type(self).__name__, ", ".join(sorted(repr(k) for k in self.keys()))
        )

    def copy(self) -> "DotDict":
        """
        Creates a shallow copy of the current `DotDict`.
        """
        return type(self)(self.__dict__.copy())

    def to_dict(self) -> Dict:
        """
        Converts the current `DotDict` (and any nested `DotDict` items)
        to an appropriately nested dictionary
        """
        # Mypy cast
        return cast(dict, as_nested_dict(self, dct_class=dict))


def as_nested_dict(
    obj: Union[DictLike, Iterable[DictLike]], dct_class: type = DotDict
) -> Union[DictLike, Iterable[DictLike]]:
    """
    Given an object formatted as a dictionary, transforms it (and any nested dictionaries)
    into the provided `dct_class`.

    Args:
        - obj (Union[DictLike, Iterable[DictLike]]): An object that's formatted as a dict or
            iterable of dicts.
        - dct_class (type): The target dict class to use (defaults to DotDict)

    Returns:
        - Union[DictLike, Iterable[DictLike]]: obj formatted as `dct_class`, or an
            iterable of `dct_class`.
    """

    # Iterable of dict to apply to each element
    if isinstance(obj, (list, tuple, set)):
        return type(obj)([as_nested_dict(d, dct_class) for d in obj])

    elif isinstance(obj, (dict, DotDict)):
        # DoctDicts could have keys that shadow `update` and `items`, so we
        # peek into `__dict__` to avoid uses where those are overriden.

        return dct_class(
            {
                k: as_nested_dict(v, dct_class)
                for k, v in getattr(obj, "__dict__", obj).items()
            }
        )

    # Handle cases where the object isn't actually a dictlike or iterable
    # of dictlike, just to make this safe to call recursively on entire dicts.
    return obj


def merge_dicts(d1: DictLike, d2: DictLike) -> DictLike:
    """
    Updates `d1` from `d2` by replacing each `(k, v1)` pair in `d1`
    with the corresponding `(k, v2)` pair in `d2`.

    If the value of each pair is itself a dict, then that value
    is updated recursively.

    Args:
        - d1 (MutableMapping): A dictionary to be updated
        - d2 (MutableMapping): A dictionary used for replacement

    Returns:
        - A `MutableMapping` with the two dict's content merged
    """

    new_mapping = d1.copy()
    for k, v in d2.items():
        if isinstance(new_mapping.get(k), MutableMapping) and isinstance(
            v, MutableMapping
        ):
            new_mapping[k] = merge_dicts(new_mapping[k], d2[k])
        else:
            new_mapping[k] = d2[k]

    return new_mapping


def dict_to_flatdict(dct: dict, parent: CompoundKey = None) -> dict:
    """Converts a (nested) dictionary to a flattened representation.
    Each key of the flat dict will be a CompoundKey tuple containing the "chain of keys"
    for the corresponding value.
    Args:
        - dct (dict): The dictionary to flatten
        - parent (CompoundKey, optional): Defaults to `None`. The parent key
        (you shouldn't need to set this)
    Returns:
        - dict: A flattened dict
    """

    items = []  # type: list
    parent = parent or CompoundKey()
    for k, v in dct.items():
        k_parent = CompoundKey(parent + (k,))
        if isinstance(v, dict):
            items.extend(dict_to_flatdict(v, parent=k_parent).items())
        else:
            items.append((k_parent, v))
    return dict(items)


def flatdict_to_dict(dct: dict, dct_class: type = None) -> MutableMapping:
    """
    Converts a flattened dictionary back to a nested dictionary.
    
    Args:
        - dct (dict): The dictionary to be nested. Each key should be a
        `CompoundKey`, as generated by `dict_to_flatdict()`
        - dct_class (type, optional): the type of the result; defaults to `dict`
    Returns:
        - MutableMapping: A `MutableMapping` used to represent a nested dictionary
    """

    result = (dct_class or dict)()  # type: MutableMapping
    for k, v in dct.items():
        if isinstance(k, CompoundKey):
            current_dict = result
            for ki in k[:-1]:
                current_dict = current_dict.setdefault(  # type: ignore
                    ki, (dct_class or dict)()
                )
            current_dict[k[-1]] = v
        else:
            result[k] = v

    return result
