from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/jeantozzi/data-engineering-zoomcamp-2023.git"
)
block.save("github", overwrite=True)