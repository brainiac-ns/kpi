import argparse

from factory import Factory

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description")
    parser.add_argument("--job_name", type=str)

    args = parser.parse_args()
    factory = Factory(args.job_name)
    factory()
