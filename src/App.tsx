import { useState } from "react";
import "./App.css";
import { Sidebar } from './Sidebar';
import { Page } from './Page';
import { Popup, PopupProps } from "./popup/Popup";
import { Dialog, DialogBody, DialogHeader, Typography } from "@material-tailwind/react";


function App() {
  const [selectedSchema, setSelectedSchema] = useState<{ oid: number, name: string } | null>(null);
  const [popup, setPopup] = useState<PopupProps>({ popup: 'none' });
  const [err, setErr] = useState<{ message: string, stack: string | undefined } | null>(null);

  /**
   * Closes the current popup.
   */
  function onClosePopup() {
    setPopup({ popup: 'none' });
  }

  /**
   * Displays a popup for an error.
   */
  function onError(e: unknown) {
    if (e instanceof Error) {
      setErr({ message: e.message, stack: e.stack });
    } else {
      const str: string = `${e}`;
      let components: string[] = str.split('===== STACK =====', 2);
      if (components.length > 1) {
        setErr({ message: components[0], stack: components[1] });
      } else if (components.length > 0) {
        setErr({ message: components[0], stack: undefined });
      }
    }
  }

  return (
    <main>
      <div className="fixed left-0 right-0 top-0 bottom-0 flex flex-row">
        <Sidebar 
          selectedSchemaOid={selectedSchema?.oid ?? null} 
          onSelectSchema={(schemaOid, schemaName) => { setSelectedSchema({ oid: schemaOid, name: schemaName }); }}
          onRequestCreateSchema={(defaultSchemaType) => { 
            setPopup({ 
              popup: 'createSchema', 
              defaultSchemaType,
              onClosePopup,
              onError,
            }); 
          }} 
        />
        <Page
          schema={selectedSchema}
          onRequestCreateColumn={(schema, isTableColumn, ordering) => { 
            setPopup({ 
              popup: 'createColumn',
              schema,
              isTableColumn, 
              ordering,
              onClosePopup,
              onError,
            }); 
          }}
          onRequestEditColumn={(columnMetadata, isTableColumn) => { 
            setPopup({ 
              popup: 'editColumn',
              columnMetadata,
              isTableColumn,
              onClosePopup,
              onError,
            });
          }}
        />
      </div>
      <Popup {...popup} />
      <Dialog open={err !== null} handler={() => { setErr(null); }}>
          <DialogHeader>Error</DialogHeader>
          <DialogBody>
            <Typography variant="small">{err?.message}</Typography>
            <Typography variant="small">{err?.stack}</Typography>
          </DialogBody>
      </Dialog>
    </main>
  );
}

export default App;
