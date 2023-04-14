from cx_Freeze import setup, Executable
import sys

build_exe_options = {
    'packages': ['pandas', 'names', 'os'],
    'excludes': ['tkinter', 'test', 'unittest', 'numpy'],
}

base = None
if sys.platform == 'win32':
    base = 'Win32GUI'

executables = [
    Executable('anonymize_data.py', base=base)
]

setup(
    name='AnonymizeData',
    version='0.1',
    description='Anonymize patient data',
    options={'build_exe': build_exe_options},
    executables=executables,
)
