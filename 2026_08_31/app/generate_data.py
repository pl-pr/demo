import os
import numpy as np
import pandas as pd


OUTPUT_DIR = "/data"

ROWS = 20_000_000
CHUNK_SIZE = 1_000_000


def generate_chunk(start, size):

    rng = np.random.default_rng(start)

    return pd.DataFrame({
        "login": rng.integers(
            1,
            1_000_000,
            size=size,
            dtype=np.int64
        ),
        "branch": rng.choice(
            ["PLPL", "PLDE", "PLFR"],
            size=size
        ),
        "lots": rng.integers(
            1,
            10,
            size=size,
            dtype=np.int32
        ),
    })


def main():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for i in range(0, ROWS, CHUNK_SIZE):

        size = min(CHUNK_SIZE, ROWS - i)
        df = generate_chunk(i, size)
        path = (
            f"{OUTPUT_DIR}/"
            f"chunk_{i // CHUNK_SIZE:03d}.parquet"
        )
        df.to_parquet(
            path,
            engine="pyarrow",
            index=False
        )

if __name__ == "__main__":
    main()

