"""Package verified outputs and actual screenshots; never generate fake evidence."""
from pathlib import Path
from datetime import datetime
import subprocess
import sys
import zipfile

root = Path(__file__).resolve().parent
subprocess.run([sys.executable, str(root / 'verify.py'),
                str(root / 'output/output_task1/part-r-00000'),
                str(root / 'output/output_task2/part-r-00000')], check=True)
if not (root / 'target/stock-set-jobs-1.0.jar').is_file():
    raise SystemExit('Missing JAR: run bash build.sh first.')
screenshots = [p for p in (root / 'screenshots').glob('*')
               if p.suffix.lower() in {'.png', '.jpg', '.jpeg'}]
if not screenshots:
    raise SystemExit('Add real successful YARN Web screenshots to screenshots/ first.')
dest = root.parent / ('241275017-homework5-' + datetime.now().strftime('%Y%m%d-%H%M%S') + '.zip')
with zipfile.ZipFile(dest, 'w', zipfile.ZIP_DEFLATED) as archive:
    for p in root.rglob('*'):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if any(x in {'.git', '.build', '__pycache__', 'runs'} for x in rel.parts):
            continue
        if p.suffix == '.crc' or (rel.parts[0] == 'target' and p.suffix != '.jar'):
            continue
        archive.write(p, Path('homework5') / rel)
print('PACKAGE_OK:', dest)
