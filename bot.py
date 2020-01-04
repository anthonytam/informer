from informer import TGInformer
import sys
import yaml
# ===========
# Quick setup
# ===========

#virtualenv venv
#source venv/bin/activate
#pip install -r requirements.txt

# Read more: https://github.com/paulpierre/informer/


if __name__ == '__main__':
    try:
        account_id = sys.argv[1]
    except:
        sys.exit('usage: %s account_id' % sys.argv[0])

    if not account_id:
        sys.exit('Account ID required')

    config = None
    with open("config.yaml", "r") as conf_file:
        config = yaml.load(conf_file, Loader=yaml.FullLoader)

    informer = TGInformer(account_id, config["config"])
    # Blocks unless the informer crashes
    informer.init()
    sys.exit(0)
