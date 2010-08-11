#!/bin/bash

###################### you may want to tweak this part: ######################


DIR_NUKE='0'                # '1' - removes DIR_MAIN before install
                            # '0' - bail if DIR_MAIN already exists

DIR_LINK='1'                # '1' - soft-link Pydra (useful for development)
                            # '0' - copy Pydra (configs are *always* copied)


DIR_MAIN="$HOME/.pydra"

DIR_SRC="$DIR_MAIN/src"     # Pydra source .py scripts

DIR_BIN="$DIR_MAIN/bin"     # master and node binaries
DIR_WEB="$DIR_MAIN/web"     # Django manager's scripts

DIR_CFG="$DIR_MAIN/etc"     # main configuration files
DIR_LIB="$DIR_MAIN/lib"     # tasks, etc. (-> RUNTIME_FILES_DIR)
DIR_LOG="$DIR_MAIN/log"     # logs / dump (-> LOG_DIR, LOG_ARCHIVE)
DIR_PID="$DIR_MAIN/run"     # process ids (-> RUNTIME)


TIMEZONE=`cat /etc/timezone || echo "GMT"`  # You can override this automagic.


################### <- don't edit stuff below this line -> ###################


echo ""
echo "This alternative setup script can be used to install Pydra in user's /home."
echo "You probably want to edit it first though. If you forgot, press ^C to exit."
echo ""

if  [[ "$DIR_NUKE" == "1" ]]
then
    echo "This will REMOVE ALL the files under: $DIR_MAIN"
else
    echo "This WON'T overwrite any files under: $DIR_MAIN"
fi

if  [[ "$DIR_LINK" == "1" ]]
then
    echo "Pydra's source files will be LINKED."
    
    function CMD_FILE
    {
        ln -s `readlink -f "$1"` "$2"
    }
    function CMD_DIRS
    {
        ln -s `readlink -f "$1"` "$2"
    }
else
    echo "Pydra's source files will be COPIED."
    
    function CMD_FILE
    {
        cp    "$1" "$2"
    }
    function CMD_DIRS
    {
        cp -r "$1" "$2"
    }
fi

echo ""
echo "Check your settings and press Enter continue... Otherwise press ^C to exit."
echo ""

read -s


if  [[ ! ( -e 'setup.py' && -d 'pydra' &&  -d 'config' && -d 'examples' ) ]]
then
    cd `dirname "$0"`
    cd ..
fi

if  [[ ! ( -e 'setup.py' && -d 'pydra' &&  -d 'config' && -d 'examples' ) ]]
then
    echo "$0: needs to be run from pydra base directory" >&2
    exit 1
fi


if  [[ -d "$DIR_MAIN" && ( "$DIR_NUKE" == 1 ) ]]
then
    rm -r "$DIR_MAIN"
fi


mkdir -v "$DIR_MAIN" || exit 1

mkdir -v "$DIR_SRC"
mkdir -v "$DIR_BIN"
mkdir -v "$DIR_WEB"
mkdir -v "$DIR_CFG"
mkdir -v "$DIR_LIB"
mkdir -v "$DIR_LOG"
mkdir -v "$DIR_PID"

mkdir -v "$DIR_LIB/tasks"
mkdir -v "$DIR_LIB/tasks_internal"
mkdir -v "$DIR_LIB/tasks_sync_cache"


CMD_DIRS "./pydra"                          "$DIR_SRC/pydra"
CMD_DIRS "./examples/demo"                  "$DIR_LIB/tasks/demo" 

CMD_FILE "./scripts/pydra_master"           "$DIR_BIN/"
CMD_FILE "./scripts/pydra_node"             "$DIR_BIN/"

CMD_FILE "./scripts/pydra_manage"           "$DIR_WEB/"
CMD_FILE "./scripts/pydra_web_manage"       "$DIR_WEB/"

CMD_FILE "./config/pydra_settings.py"       "$DIR_CFG/"
CMD_FILE "./config/pydra_web_settings.py"   "$DIR_CFG/"


SECRET_A=`dd if=/dev/urandom bs=1 count=48 status=noxfer 2> /dev/null | base64`
SECRET_B=`dd if=/dev/urandom bs=1 count=48 status=noxfer 2> /dev/null | base64`

sed -r "s:^(RUNTIME_FILES_DIR = ).*:\1'$DIR_LIB':"  -i "$DIR_CFG/pydra_settings.py" # it's a kind of magic,
sed -r "s:^(RUNTIME = ).*:\1'$DIR_PID':"            -i "$DIR_CFG/pydra_settings.py" #             ...magic,
sed -r "s:^(LOG_DIR = ).*:\1'$DIR_LOG':"            -i "$DIR_CFG/pydra_settings.py" #             ...MAGIC!

sed -r "s:^(TIME_ZONE = ).*:\1'$TIMEZONE':"         -i "$DIR_CFG/pydra_settings.py"
sed -r "s:^(TIME_ZONE = ).*:\1'$TIMEZONE':"         -i "$DIR_CFG/pydra_web_settings.py"

sed -r "s:^(SECRET_KEY = ).*:\1'$SECRET_A':"        -i "$DIR_CFG/pydra_settings.py"
sed -r "s:^(SECRET_KEY = ).*:\1'$SECRET_B':"        -i "$DIR_CFG/pydra_web_settings.py"

# note that sed -i "unwinds" any soft links, so it doesn't matter if we linked
# the config files beforehand - this is why they're copied for DIR_LINK=1, too



echo ""
echo ""
echo "Now we're going to prepare Pydra's backend database. The default is sqlite."
echo "You'll also be asked to create the main user account for the web interface."
echo ""
echo "Check your settings and press Enter continue... Otherwise press ^C to exit."
echo ""

read -s

export PYTHONPATH="$DIR_SRC:$DIR_CFG"

"$DIR_WEB/pydra_manage"     syncdb

echo ""

"$DIR_WEB/pydra_web_manage" syncdb


echo ""
echo ""
echo "Finally, we need to create two TLS certificates for the worker and node(s)."

if  [[ -x /usr/bin/certtool ]]
then
    echo "We need certtool (from GnuTLS) for this, and you seem to have it, allright."
else
    echo "We need certtool (from GnuTLS) for this, and you DON'T have it. Oh, bummer."
    echo "You can press ^C and use generate-cert.sh later, or you can install GnuTLS."
fi

echo ""
echo "Both certificate files will go under: $DIR_LIB"

echo ""
echo "Check your settings and press Enter continue... Otherwise press ^C to exit."
echo ""

read -s

certtool --generate-privkey --outfile "$DIR_LIB/ca-key.pem"
certtool --generate-self-signed --load-privkey "$DIR_LIB/ca-key.pem" --outfile "$DIR_LIB/ca-cert.pem"


echo ""
echo ""
echo "We're finished, but you still need to put Pydra's files in your PYTHONPATH."
echo "The simplest solution is to execute the following line from a bash session:"
echo ""
echo "export PYTHONPATH=\"$DIR_SRC:$DIR_CFG:\$PYTHONPATH\""
echo ""
echo ""
echo "Lost? You probably want to run pydra_master and pydra_node first (first run"
echo "takes a while because we need to generate some extra keys). Once they're up"
echo "run 'pydra_web_manage runserver', log in, add the local node and test away!"
echo ""
