from setuptools import setup

setup(
    version="3.4.0",
    name="oai-pmh-extractor",
    description="harvest metadata and extract payload via an OAI-PMH interface",
    author="LZV.nrw",
    install_requires=[
        "requests==2.*",
        "xmltodict==0.*",
        "lxml==5.*",
        "dcm-common>=3.0.0,<4.0.0",
    ],
    packages=[
        "oai_pmh_extractor",
    ],
    package_data={"oai_pmh_extractor": ["py.typed"]},
    setuptools_git_versioning={
        "enabled": True,
        "version_file": "VERSION",
        "count_commits_from_version_file": True,
        "dev_template": "{tag}.dev{ccount}",
    },
)
