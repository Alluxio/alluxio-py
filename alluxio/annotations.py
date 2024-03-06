from typing import Optional


def PublicAPI(*args, **kwargs):
    """Annotation for documenting public APIs.

    Public APIs are classes and methods exposed to end users of Alluxio Client.

    If ``stability="alpha"``, the API can be used by advanced users who are
    tolerant to and expect breaking changes.

    If ``stability="beta"``, the API is still public and can be used by early
    users, but are subject to change.

    If ``stability="stable"``, the APIs will remain backwards compatible across
    minor Alluxio releases (e.g., Alluxio 1.1 -> 1.0).

    Args:
        stability: One of {"stable", "beta", "alpha"}.

    Examples:
        >>> from alluxio.annotations import PublicAPI
        >>> @PublicAPI
        ... def func(x):
        ...     return x

        >>> @PublicAPI(stability="beta")
        ... def func(y):
        ...     return y
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return PublicAPI(stability="stable")(args[0])

    if "stability" in kwargs:
        stability = kwargs["stability"]
        assert stability in ["stable", "beta", "alpha"], stability
    elif kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs.keys()))
    else:
        stability = "stable"

    def wrap(obj):
        if stability in ["alpha", "beta"]:
            message = (
                f"**PublicAPI ({stability}):** This API is in {stability} "
                "and may change before becoming stable."
            )
            _append_doc(obj, message=message)

        _mark_annotated(obj)
        return obj

    return wrap


def _append_doc(obj, *, message: str, directive: Optional[str] = None) -> str:
    if not obj.__doc__:
        obj.__doc__ = ""

    obj.__doc__ = obj.__doc__.rstrip()

    indent = _get_indent(obj.__doc__)
    obj.__doc__ += "\n\n"

    if directive is not None:
        obj.__doc__ += f"{' ' * indent}.. {directive}::\n\n"

        message = message.replace("\n", "\n" + " " * (indent + 4))
        obj.__doc__ += f"{' ' * (indent + 4)}{message}"
    else:
        message = message.replace("\n", "\n" + " " * (indent + 4))
        obj.__doc__ += f"{' ' * indent}{message}"
    obj.__doc__ += f"\n{' ' * indent}"


def _mark_annotated(obj) -> None:
    # Set magic token for check_api_annotations linter.
    if hasattr(obj, "__name__"):
        obj._annotated = obj.__name__


def _get_indent(docstring: str) -> int:
    """

    Example:
        >>> def f():
        ...     '''Docstring summary.'''
        >>> f.__doc__
        'Docstring summary.'
        >>> _get_indent(f.__doc__)
        0

        >>> def g(foo):
        ...     '''Docstring summary.
        ...
        ...     Args:
        ...         foo: Does bar.
        ...     '''
        >>> g.__doc__
        'Docstring summary.\\n\\n    Args:\\n        foo: Does bar.\\n    '
        >>> _get_indent(g.__doc__)
        4

        >>> class A:
        ...     def h():
        ...         '''Docstring summary.
        ...
        ...         Returns:
        ...             None.
        ...         '''
        >>> A.h.__doc__
        'Docstring summary.\\n\\n        Returns:\\n            None.\\n        '
        >>> _get_indent(A.h.__doc__)
        8
    """
    if not docstring:
        return 0

    non_empty_lines = list(filter(bool, docstring.splitlines()))
    if len(non_empty_lines) == 1:
        # Docstring contains summary only.
        return 0

    # The docstring summary isn't indented, so check the indentation of the second
    # non-empty line.
    return len(non_empty_lines[1]) - len(non_empty_lines[1].lstrip())
