; Query endpoint status given the endpoint as the first arg
(in-ns 'org.corfudb.shell) ; so our IDE knows what NS we are using

(import org.docopt.Docopt) ; parse some cmdline opts
(require 'clojure.pprint)
(require 'clojure.java.shell)
(def usage "corfu_layouts, work with the Corfu layout view.
Usage:
  corfu_layouts -c <config> query
  corfu_layouts -c <config> edit
Options:
  -c <config>, --config <config>              Configuration string to use.
  -h, --help     Show this screen.
")

; Parse the incoming docopt options.
(def localcmd (.. (new Docopt usage) (parse *args)))

; Get the runtime.
(get-runtime (.. localcmd (get "--config")))
(connect-runtime)
(def layout-view (get-layout-view))

(defn install-layout
  "Install a new layout"
  [layout]
  ; For now, we start at rank 0, but we really should get the highest rank proposed
  (loop [layout-rank 0]
    (when (> layout-rank -1)
      (do
        (println (str "Trying install with rank " layout-rank))
        (recur (try
         (do
           (.. (get-layout-view) (updateLayout layout layout-rank))
           -1)
         (catch org.corfudb.runtime.exceptions.OutrankedException e
                (inc layout-rank))))
   )))
  )
(defn print-layout [] (pprint-json (.. (.. (get-layout-view) (getLayout)) (asJSONString))))
(defn edit-layout [] (let [layout (.. (get-layout-view) (getLayout))
                           temp-file (java.io.File/createTempFile "corfu" ".tmp")]
                       ; Write the layout into a temp file
                       (do (.deleteOnExit temp-file)
                           (with-open [file (clojure.java.io/writer temp-file)]
                              (binding [*out* file]
                                (pprint-json (.. layout (asJSONString)))))
                           ; open the editor
                           (edit-file (.getAbsolutePath temp-file))
                           ; read in the new layout
                           (let [new-layout (do (let [temp (org.corfudb.runtime.view.Layout/fromJSONString
                                                             (slurp (.getAbsolutePath temp-file)))]
                                                  ; TODO: the layout needs a runtime..., we need to fix
                                                  ; this weird broken dependency
                                                  (.. temp (setRuntime *r))
                                                  temp
                                                  ))]
                             (if (.equals layout new-layout) (println "Layout not modified, exiting")
                                 ; If changes were made, check if the layout servers were modified
                                 ; if it was, we'll have to add them to the service
                                 (if (.equals (.getLayoutServers layout) (.getLayoutServers new-layout))
                                     ; Equal, just install the new layout
                                     (do
                                      (install-layout new-layout)
                                      (println "New layout installed!"))
                                     ; Not equal, need to:
                                     ; (1) make sure all layout servers are bootstrapped
                                     ; (2) install layout on all servers
                                     (do
                                       (doseq [server (.getLayoutServers new-layout)]
                                       (do (get-router server)
                                           (.bootstrapLayout (get-layout-client) new-layout)))
                                       (install-layout new-layout)
                                       (println "New layout installed!")
                                     )
                                 )
                            )
                       ))))

; determine whether to read or write
(cond (.. localcmd (get "query")) (print-layout)
  (.. localcmd (get "edit")) (edit-layout)
  :else (println "Unknown arguments.")
  )
