def cut(s,a,b,new=''):
    i=s.index(a); j=s.index(b,i)
    return s[:i]+new+s[j:]
def edit(p,f):
    s=open(p).read(); s=f(s); open(p,'w').write(s)
def aa(s):
    s=s.replace("# RocksDB's identity source, and the online backup root on the NAS (#156), which its backup\n# thread writes and its offline restore reads.\n","# RocksDB's identity source.\n")
    s=cut(s,"  # The online backup root (backup_root, #156)","  # Its own descriptors")
    s=s.replace("  # The online backup reads the mount table to refuse a backup root that is not on a network\n  # filesystem apart from the store's (#156).\n  @{PROC}/@{pid}/mountinfo r,\n","")
    return s
edit('deploy/apparmor/sparrow',aa)
def svc(s):
    s=cut(s,"# The store and socket, the consume roots, and the online backup root","ReadWritePaths=","# The store and socket and the consume roots.\n")
    s=s.replace("ReadWritePaths=/SPARROW/store /SPARROW/intake -/net/synology/archives/sparrow","ReadWritePaths=/SPARROW/store /SPARROW/intake")
    s=cut(s,"# The online backup thread lowers its own priority","\n[Install]")
    s=s.replace("SystemCallFilter=~@privileged @resources\n","SystemCallFilter=~@privileged @resources\n",1)
    return s
edit('deploy/systemd/sparrow.service',svc)
def toml(s):
    return cut(s,"# Online backups (#156)","# Consume watches live")
edit('deploy/config/server.toml.example',toml)
edit('deploy/config/tmpfiles-sparrow.conf',lambda s:s.replace("# The shipper's lock (the online backup takes no lock the shipper holds). It must exist","# The shipper's lock. It must exist"))
edit('deploy/libexec/sparrow-ship',lambda s:s.replace("so the backup's drain wait stays bounded","so a store stop's drain wait stays bounded").replace("    # The backup holds this lock while it copies the intake roots, queues and ledgers.\n","    # One shipper runs at a time.\n"))
