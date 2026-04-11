import os
import hmac
import hashlib
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

def derive_keys(master_key_str, episode_token, clip_id):
    master_key = master_key_str.encode('utf-8')
    episode_token_bytes = episode_token.encode('utf-8')

    # Episode Key
    h_key = hmac.new(master_key, episode_token_bytes, hashlib.sha256)
    clip_key = bytes.fromhex(h_key.hexdigest())

    # Master IV Seed
    h_iv_seed = hmac.new(master_key, episode_token_bytes + b'_seed', hashlib.sha256)
    iv_seed_bytes = bytes.fromhex(h_iv_seed.hexdigest())

    # Unique IV for clip
    clip_id_bytes = clip_id.encode('utf-8')
    h_iv = hmac.new(iv_seed_bytes, clip_id_bytes, hashlib.sha256)
    iv = bytes.fromhex(h_iv.hexdigest()[:32])

    return clip_key, iv

def encrypt_file(file_path, master_key_str, episode_token, clip_id):
    clip_key, iv = derive_keys(master_key_str, episode_token, clip_id)
    cipher = Cipher(algorithms.AES(clip_key), modes.CTR(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    out_path = file_path.replace('.mp4', '.bin') if file_path.endswith('.mp4') else file_path.replace('.webp', '.ebp') if file_path.endswith('.webp') else file_path + '.bin'

    with open(file_path, 'rb') as f_in, open(out_path, 'wb') as f_out:
        while True:
            chunk = f_in.read(1024 * 1024)
            if not chunk: break
            f_out.write(encryptor.update(chunk))
        f_out.write(encryptor.finalize())
    return out_path
