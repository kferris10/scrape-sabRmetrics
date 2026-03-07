FROM rocker/r-ver:4.4.3

# System libraries needed by R packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libpq-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy renv infrastructure first — changes here bust the package cache layer
COPY renv.lock renv.lock
COPY .Rprofile .Rprofile
COPY renv/activate.R renv/activate.R
COPY renv/settings.json renv/settings.json

# Install renv, then restore all packages from the lockfile.
# bigrquery is installed separately because it is not yet in renv.lock;
# after running `renv::install("bigrquery"); renv::snapshot()` locally and
# committing the updated renv.lock, this extra line can be removed.
RUN Rscript -e 'install.packages("renv", repos = "https://cloud.r-project.org")' \
 && Rscript -e 'renv::restore()' \
 && Rscript -e 'if (!requireNamespace("bigrquery", quietly = TRUE)) install.packages("bigrquery", repos = "https://cloud.r-project.org")'

# Copy source (after packages — avoids reinstalling on code changes)
COPY R/        R/
COPY sql/      sql/
COPY config.yml config.yml

ENTRYPOINT ["Rscript", "R/run_scrape.R"]
CMD ["--mode", "daily", "--backend", "bigquery"]
