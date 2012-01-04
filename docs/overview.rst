.. Vumi system overview

Vumi Overview
=============

.. tikz:: A simple Vumi worker setup
   :filename: images/tikz/vumi-simple-setup.png
   :libs: arrows,shadows,decorations.pathmorphing,shapes,positioning

   \tikzstyle{place}=[double copy shadow,
                      shape=rounded rectangle,
                      thick,
                      inner sep=0pt,
                      outer sep=0.5ex,
                      minimum height=2em,
                      minimum width=10em,
                      node distance=10em,
                     ];

   \tikzstyle{rabbit}=[->,
                       >=stealth,
                       line width=0.2ex,
                       auto,
                       ];

   \tikzstyle{route}=[sloped,midway,above=0.1em];
   \tikzstyle{outbound}=[draw=black!50]
   \tikzstyle{inbound}=[draw=black]
   \tikzstyle{failure}=[draw=black, decorate, decoration={snake,pre length=1mm,post length=1mm}]

   \definecolor{darkred}{rgb}{0.5,0,0}
   \definecolor{darkgreen}{rgb}{0,0.5,0}
   \definecolor{darkblue}{rgb}{0,0,0.5}

   \node[place,draw=darkred!50,fill=darkred!20]  (failure_worker) {Failure Workers};
   \node[place,draw=darkblue!50,fill=darkblue!20]  (transport)  [below=of failure_worker] {Transports};
   \node[place,draw=darkgreen!50,fill=darkgreen!20]  (app_worker) [right=of transport] {Application Workers};

   \draw[rabbit,inbound] (transport) to node [route] {inbound} (app_worker);
   \draw[rabbit,inbound,bend right] (transport) to node [route] {event} (app_worker);
   \draw[rabbit,outbound,bend right] (app_worker) to node [route] {outbound} (transport);

   \draw[rabbit,failure,bend right] (transport) to node [route] {failure} (failure_worker);
   \draw[rabbit,outbound,bend right] (failure_worker) to node [route] {outbound} (transport);
