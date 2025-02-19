import subprocess

import fire

import download_files
import download_files_kyd
import download_files_selenium
import processor


def kyd():
    print('download_kyd')
    download_files_kyd.main()


def legacy():
    print('download_legacy')
    download_files.main()


def selenium():
    print('download_selenium')
    download_files_selenium.main()


def powershell():
    print('download_powershell')
    subprocess.run(['pwsh', '-File', './download_files.ps1'], check=True)


def all():
    print('download_all')
    kyd()
    legacy()
    # selenium()
    powershell()


def processor():
    print('processor')
    processor.main()


if __name__ == '__main__':
    fire.Fire()
