from setuptools import setup

setup(
    name='acag_metfield_pipeline',
    version='0.0.0',
    author="Liam Bindle",
    author_email="liam.bindle@gmail.com",
    description="Luigi-based pipeline for downloading and preprocessing metfields.",
    url="todo",
    project_urls={
        "Bug Tracker": "todo",
    },
    packages=['acag_metfield_pipeline'],
    install_requires=[
        'requests',
        'luigi',
        'fv3_mass_flux_tools',
    ],
)
