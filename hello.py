import re

s = "system.heightening.levels.damage.1.type"
pattern = re.compile(r"^(.+?)\.(\d+)\.(.+)$")
print(pattern.match(s))