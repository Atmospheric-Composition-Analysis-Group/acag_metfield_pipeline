FROM python:3.10

RUN pip install git+https://github.com/LiamBindle/fv3_mass_flux_tools.git

COPY . /acag_metfield_pipeline
COPY luigi_compute1.cfg /acag_metfield_pipeline/luigi.cfg

RUN pip install /acag_metfield_pipeline

WORKDIR /acag_metfield_pipeline

CMD bash
