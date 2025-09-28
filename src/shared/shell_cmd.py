import subprocess

# ============================== PUBLIC ============================== #


def shell_silent(cmd_string: str) -> int:
    sp = subprocess.run(
        cmd_string,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        shell=True,
    )
    return sp.returncode
