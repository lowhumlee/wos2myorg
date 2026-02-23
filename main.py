import typer
from pathlib import Path
from app.pipeline import run_pipeline

app = typer.Typer(help="MUV WoS MyOrg ingestion tool")

@app.command()
def run(
    input_file: Path = typer.Argument(..., help="New WoS export CSV"),
    existing: Path = typer.Option("data/ResearcherAndDocument.csv"),
    orgs: Path = typer.Option("data/OrganizationHierarchy.csv"),
    config: Path = typer.Option("config.yaml"),
    out_dir: Path = typer.Option("output")
):
    run_pipeline(input_file, existing, orgs, config, out_dir)

if __name__ == "__main__":
    app()