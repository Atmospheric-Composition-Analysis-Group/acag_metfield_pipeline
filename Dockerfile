FROM python:3.10

RUN pip install git+https://github.com/LiamBindle/fv3_mass_flux_tools.git
RUN pip install git+https://github.com/LiamBindle/sparselt.git

COPY . /acag_metfield_pipeline

RUN pip install /acag_metfield_pipeline

WORKDIR /acag_metfield_pipeline

CMD bash
