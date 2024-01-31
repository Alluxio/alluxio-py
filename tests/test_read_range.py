import argparse
import os
import random

from alluxio import AlluxioFileSystem


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate Alluxio read_range with local file."
    )
    parser.add_argument(
        "--alluxio_file_path",
        default="s3://ai-ref-arch/small-dataset/iris.csv",
        required=False,
        help="The Alluxio file path to read",
    )
    parser.add_argument(
        "--local_file_path",
        default="/Users/alluxio/Downloads/iris.csv",
        required=False,
        help="The local file path to validate against",
    )
    parser.add_argument(
        "--etcd_hosts",
        type=str,
        default="localhost",
        required=False,
        help="The host address(es) for etcd",
    )
    parser.add_argument(
        "--num_tests",
        type=int,
        default=100,
        required=False,
        help="The total number of read range test to run",
    )
    return parser.parse_args()


def validate_read_range(
    alluxio_fs, alluxio_file_path, local_file_path, offset, length
):
    alluxio_data = alluxio_fs.read_range(alluxio_file_path, offset, length)

    with open(local_file_path, "rb") as local_file:
        local_file.seek(offset)
        local_data = local_file.read(length)

    try:
        assert alluxio_data == local_data
    except AssertionError:
        error_message = (
            f"Data mismatch between Alluxio and local file\n"
            f"Alluxio file path: {alluxio_file_path}\n"
            f"Local file path: {local_file_path}\n"
            f"Offset: {offset}\n"
            f"Length: {length}\n"
            f"Alluxio data: {alluxio_data}\n"
            f"Local data: {local_data}"
        )
        raise AssertionError(error_message)


def manual_test_invalid_read_range(
    alluxio_fs, alluxio_file_path, local_file_path, offset, length
):
    try:
        alluxio_fs.read_range(alluxio_file_path, offset, length)
    except Exception:
        pass
    else:
        raise AssertionError(
            "Expected an exception from Alluxio but none occurred."
        )

    try:
        with open(local_file_path, "rb") as local_file:
            local_file.seek(offset)
            local_file.read(length)
    except Exception:
        pass
    else:
        raise AssertionError(
            "Expected an exception from local file read but none occurred."
        )


def main(args):
    alluxio_fs = AlluxioFileSystem(etcd_hosts=args.etcd_hosts)
    file_size = os.path.getsize(args.local_file_path)

    invalid_test_cases = [(-1, 100), (file_size - 1, -2)]
    for offset, length in invalid_test_cases:
        manual_test_invalid_read_range(
            alluxio_fs,
            args.alluxio_file_path,
            args.local_file_path,
            offset,
            length,
        )
    print("Passed invalid test cases")

    # Validate normal case
    max_length = 13 * 1024 * 1024
    for _ in range(args.num_tests):
        offset = random.randint(0, file_size - 1)
        length = min(random.randint(-1, file_size - offset), max_length)
        # -1 and None length represents read from offset to file end
        if length == 0:
            length = None
        validate_read_range(
            alluxio_fs,
            args.alluxio_file_path,
            args.local_file_path,
            offset,
            length,
        )

    print(
        f"Data matches between Alluxio file and local source file for {args.num_tests} times"
    )

    special_test_cases = [
        (file_size - 1, -1),
        (file_size - 1, None),
        (file_size - 1, file_size + 1),
        (file_size, 100),
    ]

    for offset, length in special_test_cases:
        validate_read_range(
            alluxio_fs,
            args.alluxio_file_path,
            args.local_file_path,
            offset,
            length,
        )
    print("Passed corner test cases")


if __name__ == "__main__":
    args = parse_args()
    main(args)
