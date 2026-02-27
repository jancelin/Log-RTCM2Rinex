#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# scripts/gen-webstat-htpasswd.sh
#
# Génère (ou met à jour) config/webstat.htpasswd pour la Basic Auth /webstat/.
#
# Usage :
#   ./scripts/gen-webstat-htpasswd.sh <username>          # crée / écrase
#   ./scripts/gen-webstat-htpasswd.sh <username> --append # ajoute un user
#
# Exemples :
#   ./scripts/gen-webstat-htpasswd.sh webstat
#   ./scripts/gen-webstat-htpasswd.sh alice --append
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

USER_NAME="${1:-}"
APPEND="${2:-}"

if [[ -z "$USER_NAME" ]]; then
  echo "Usage: $0 <username> [--append]" >&2
  exit 1
fi

OUT="$(cd "$(dirname "$0")/.." && pwd)/config/webstat.htpasswd"
mkdir -p "$(dirname "$OUT")"

# ── Méthode 1 : htpasswd (apache2-utils / httpd) ──────────────────────────────
if command -v htpasswd >/dev/null 2>&1; then
  if [[ "$APPEND" == "--append" && -f "$OUT" ]]; then
    echo "Ajout de l'utilisateur '$USER_NAME' dans $OUT"
    htpasswd "$OUT" "$USER_NAME"
  else
    echo "Création de $OUT avec l'utilisateur '$USER_NAME'"
    htpasswd -c "$OUT" "$USER_NAME"
  fi
  echo "✓ OK. Redémarrez le container web pour appliquer : docker compose restart web"
  exit 0
fi

# ── Méthode 2 : Python (stdlib bcrypt via passlib ou crypt) ───────────────────
if command -v python3 >/dev/null 2>&1; then
  echo "htpasswd introuvable → génération via Python (format SHA-512)."
  echo "Nota : nginx accepte SHA-512 avec le module ngx_http_auth_basic_module."
  python3 - "$USER_NAME" "$OUT" "$APPEND" <<'PYEOF'
import sys, crypt, getpass, os, pathlib

user = sys.argv[1]
out  = sys.argv[2]
append = len(sys.argv) > 3 and sys.argv[3] == "--append"

pw  = getpass.getpass(f"Mot de passe pour '{user}': ")
pw2 = getpass.getpass("Confirmer : ")
if pw != pw2:
    print("Erreur : les mots de passe ne correspondent pas.", file=sys.stderr)
    sys.exit(1)

hashed = crypt.crypt(pw, crypt.mksalt(crypt.METHOD_SHA512))
line   = f"{user}:{hashed}\n"

if append and pathlib.Path(out).exists():
    # Remplace la ligne si le user existe déjà, sinon ajoute
    lines = pathlib.Path(out).read_text().splitlines(keepends=True)
    found = False
    for i, l in enumerate(lines):
        if l.startswith(user + ":"):
            lines[i] = line
            found = True
            break
    if not found:
        lines.append(line)
    pathlib.Path(out).write_text("".join(lines))
else:
    pathlib.Path(out).write_text(line)

print(f"✓ Écrit dans {out}")
PYEOF
  echo "✓ Redémarrez le container web : docker compose restart web"
  exit 0
fi

echo "ERROR: ni 'htpasswd' ni 'python3' trouvés." >&2
echo "Installez apache2-utils (Ubuntu) ou httpd-tools (RHEL) et réessayez." >&2
exit 1
