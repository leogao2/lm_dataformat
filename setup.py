import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lm_dataformat", # Replace with your own username
    version="0.0.15",
    author="Leo Gao",
    author_email="leogao31@gmail.com",
    description="A utility for storing and reading files for LM training.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/leogao2/lm_dataformat",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'zstandard',
        'jsonlines'
    ]
)
