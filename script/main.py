from eth_abi.packed import encode_abi_packed
from eth_utils import encode_hex

from script.sql import read_from_db, write_into_db
from script.merkle import MerkleTree


local_path = os.getcwd()
if not local_path in sys.path:
    sys.path.append(local_path)

def get_proof():
    elements = [
                (0, "0xC270b901392aD5D08Af9327195A2716708237A6C" , 1, 10), 
                (1, "0xf892A50951d9B2218eAe54Cf594F874746Fb800E" , 2, 10),
                (2, "0x95cAee1029D7A33ac9f848CF53266cDdFA14Dfc8" , 3, 10),
                (3, "0x0f1b63207c94Ac534CF5d2737Ae6C5B7A3AF7A47" , 2, 10),
                (4, "0xc7b50B564387F1Bf3b306958388179ffC577fB42" , 2, 10),
             ]
    nodes = [encode_hex(encode_abi_packed(['uint', 'address', 'uint', 'uint'], el)) for el in elements]
    tree = MerkleTree(nodes)
    distribution = {
        'merkleRoot': encode_hex(tree.root),
        'claims': {
            user: {'index': index, 'address': user,'gas': gas, 'total_gas':total_gas,  'proof': tree.get_proof(nodes[index])}
            for index, user, gas, total_gas in elements
        },
    }
    print(f'merkle root: {encode_hex(tree.root)}')
    return distribution

if __name__ == '__main__':
    print(get_proof())