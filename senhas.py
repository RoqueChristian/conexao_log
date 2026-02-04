from streamlit_authenticator.utilities.hasher import Hasher
                #diretoria            ce           pi                ma             pe             ba
hashes = Hasher(['Total@diretoria', 'T0t4l@c20061', 'T0t4l@90107', 'T0t4l@92288', 'T0t4l@790858', 'Total@789967']).generate()
print(hashes)