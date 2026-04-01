"""
Language registry.

To add a new language:
1. Create a file in this directory (e.g., rust.py)
2. Define a class that extends BaseLanguage
3. Import it here and add it to LANGUAGES

All other modules use get_language("python") to get the handler.
"""
from languages.python import PythonLanguage
from languages.java import JavaLanguage
from languages.cpp import CppLanguage

# Registry: maps language name -> handler instance
LANGUAGES = {
    "python": PythonLanguage(),
    "java": JavaLanguage(),
    "cpp": CppLanguage(),
}


def get_language(name: str):
    """
    Look up a language handler by name.

    Returns None if the language isn't registered.
    """
    return LANGUAGES.get(name.lower())
