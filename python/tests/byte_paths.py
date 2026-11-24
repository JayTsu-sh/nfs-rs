class BytesPath:
    """A PathLike[bytes] fixture; pathlib.Path represents text paths only."""
    def __init__(self, path: bytes):
        self.path = path

    def __fspath__(self) -> bytes:
        return self.path
