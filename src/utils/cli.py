"""Lanceur des jobs Spark en ligne de commande : le processus se termine TOUJOURS.

Constaté au jour 7 : quand le démarrage de Spark échoue, des fils d'exécution de la
JVM (heartbeat) restent actifs et Python ne se termine jamais. Sous Airflow, la tâche
restait « en cours » jusqu'à son délai maximal au lieu d'échouer et d'alerter.
"""

from __future__ import annotations

import os
import sys
import traceback
from collections.abc import Callable


def run_cli(main: Callable[[], int]) -> None:
    try:
        code = main()
    except SystemExit as exc:  # argparse, sys.exit()
        code = exc.code if isinstance(exc.code, int) else 1
    except BaseException:  # noqa: BLE001 - tout échec doit donner un code != 0
        traceback.print_exc()
        code = 1
    sys.stdout.flush()
    sys.stderr.flush()
    # os._exit : sortie immédiate, même si des fils non démons de la JVM traînent.
    # Les jobs ont déjà arrêté Spark dans leurs blocs finally.
    os._exit(code or 0)
