import re

encoder_pattern = re.compile(r'[^A-Za-z0-9_]')
decoder_pattern = re.compile(r'_x[0-9a-fA-F]{4}')

def encode_name(s: str) -> str:
    # Escape the literal "_x" to avoid confusion with escape sequences
    s = s.replace('_x', '_x005f_x0078')
    buf = []
    last_pos = 0
    matches = re.finditer(encoder_pattern, s)
    for match in matches:
        start, end = match.span()
        buf.append(s[last_pos:start])
        char = match.group()
        encoded = f"_x{ord(char):04x}"
        buf.append(encoded)
        last_pos = end

    buf.append(s[last_pos:])
    return ''.join(buf)

def decode_name(name: str) -> str:
    def replace_match(m):
        hex_code = m.group()[2:]  # strip '_x'
        return chr(int(hex_code, 16))
    return decoder_pattern.sub(replace_match, name)
