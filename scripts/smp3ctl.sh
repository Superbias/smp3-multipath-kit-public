#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${SMP3_INSTALL_TEST_ROOT:-}" ]]; then
    PREFIX="${SMP3_INSTALL_TEST_ROOT%/}/opt/smp3-standalone"
    UNIT="smp3-standalone"
else
    PREFIX="/opt/smp3-standalone"
    UNIT="smp3-standalone"
fi
INSTALLER="$PREFIX/install-smp3-server.sh"

case "${1:-status}" in
    status|check) exec "$INSTALLER" --check ;;
    start) exec systemctl start "$UNIT" ;;
    stop) exec systemctl stop "$UNIT" ;;
    restart) exec systemctl restart "$UNIT" ;;
    logs)
        shift
        if [[ "${1:-}" == "-f" ]]; then exec journalctl -u "$UNIT" -f; fi
        exec journalctl -u "$UNIT" -n 100 --no-pager
        ;;
    update) shift; exec "$INSTALLER" --update "$@" ;;
    rollback) exec "$INSTALLER" --rollback ;;
    version) exec "$PREFIX/smp3-server" -version ;;
    *)
        echo 'usage: smp3ctl status|start|stop|restart|logs [-f]|check|update|rollback|version' >&2
        exit 2
        ;;
esac
