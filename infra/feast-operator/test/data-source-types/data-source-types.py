import os
from feast.repo_config import REGISTRY_CLASS_FOR_TYPE, OFFLINE_STORE_CLASS_FOR_TYPE, ONLINE_STORE_CLASS_FOR_TYPE, LEGACY_ONLINE_STORE_CLASS_FOR_TYPE

def save_in_script_directory(filename: str, typedict: dict[str, str]):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, filename)
    
    with open(file_path, 'w') as file:
        for k in typedict.keys():
            file.write(k+"\n")

for legacyType in LEGACY_ONLINE_STORE_CLASS_FOR_TYPE.keys():
    if legacyType in ONLINE_STORE_CLASS_FOR_TYPE:
        del ONLINE_STORE_CLASS_FOR_TYPE[legacyType]

save_in_script_directory("registry.out", REGISTRY_CLASS_FOR_TYPE)
save_in_script_directory("online-store.out", ONLINE_STORE_CLASS_FOR_TYPE)
save_in_script_directory("offline-store.out", OFFLINE_STORE_CLASS_FOR_TYPE)
