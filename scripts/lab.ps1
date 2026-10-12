param(
    [Parameter(Position=0, Mandatory=$true)]
    [string]$Command,
    [Parameter(Position=1, ValueFromRemainingArguments=$true)]
    [string[]]$ArgsList
)

$PY = "/home/airflow/lab-venv/bin/python"

switch ($Command) {
    "step"   { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli step @ArgsList }
    "exp"    { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli exp @ArgsList }
    "verify" { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli verify @ArgsList }
    "test"   { docker compose exec -T -w /opt/lab airflow $PY -m unittest discover -s tests }
    "run"    { docker compose exec -T -w /opt/lab airflow scripts/run_pipeline.sh @ArgsList }
    "cli"    { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli @ArgsList }
    "dbt"    { docker compose exec -T -w /opt/dbt airflow /home/airflow/lab-venv/bin/dbt @ArgsList --no-use-colors }
    "shell"  { docker compose exec airflow bash }
    default  {
        Write-Host "Usage: .\scripts\lab.ps1 <step|exp|verify|test|run|cli|dbt|shell> [args...]"
    }
}
