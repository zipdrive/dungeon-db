import { useEffect, useState } from "react";
import "./App.css";
import { Sidebar } from './Sidebar';
import { Page } from './Page';
import { Popup, PopupBreadcrumb, PopupProps } from "./popup/Popup";
import { Button, Dialog, Typography } from "@material-tailwind/react";
import "choices.js/public/assets/styles/choices.css";
import { Menu, MenuItem, Submenu } from "@tauri-apps/api/menu";
import { loadAsync, newAsync, saveAsAsync, saveAsync } from "./api/dbfile";
import { TableMetadata } from "./api/model/table";
import { ReportMetadata } from "./api/model/report";
import { TableColumnMetadata } from "./api/model/tableColumn";


function App() {
  useEffect(() => {
    (async () => {
      const menu = await Menu.new({
        items: [
          await Submenu.new({
            text: "File",
            items: [
              await MenuItem.new({
                text: "New",
                action: async () => {
                  await newAsync();
                }
              }),
              await MenuItem.new({
                text: "Open",
                action: async () => {
                  await loadAsync();
                }
              }),
              await MenuItem.new({
                text: "Save",
                action: async () => {
                  await saveAsync();
                }
              }),
              await MenuItem.new({
                text: "Save As...",
                action: async () => {
                  await saveAsAsync();
                }
              })
            ]
          })
        ]
      });
      await menu.setAsAppMenu();
    })();
  }, []);

  const [selectedSchema, setSelectedSchema] = useState<['table' | 'report', number, string] | null>(null);
  const [popups, setPopups] = useState<PopupBreadcrumb[]>([]);
  const [err, setErr] = useState<{ message: string, stack: string | undefined } | null>(null);

  /**
   * Opens a popup.
   */
  function onOpenPopup(popup: PopupBreadcrumb) {
    setPopups((oldPopups) => oldPopups.concat([popup]));
  }

  /**
   * Opens a popup to create a new table.
   */
  function onRequestCreateTable() {
    onOpenPopup({ 
      popup: 'createTable',
      onClosePopup,
      onError,
    }); 
  }

  /**
   * Opens a popup to create a new report.
   */
  function onRequestCreateReport() {
    onOpenPopup({ 
      popup: 'createReport',
      onClosePopup,
      onError,
    }); 
  }

  /**
   * Opens a popup to edit an existing table.
   */
  function onRequestEditTable(metadata: TableMetadata) {
    onOpenPopup({ 
      popup: 'editTable',
      metadata,
      onClosePopup,
      onError,
    }); 
  }

  /**
   * Opens a popup to edit an existing report.
   */
  function onRequestEditReport(metadata: ReportMetadata) {
    onOpenPopup({ 
      popup: 'editReport',
      metadata,
      onClosePopup,
      onError,
    }); 
  }

  /**
   * Opens a popup to create a new table column.
   */
  function onRequestCreateTableColumn(tableOid: number, ordering: number | null) {
    onOpenPopup({ 
      popup: 'createTableColumn',
      tableOid,
      ordering,
      onClosePopup,
      onError,
    }); 
  }

  /**
   * Opens a popup to edit an existing table column.
   */
  function onRequestEditTableColumn(columnMetadata: TableColumnMetadata) {
    onOpenPopup({ 
      popup: 'editTableColumn',
      columnMetadata,
      onClosePopup,
      onError,
    }); 
  }

  /**
   * Opens a popup to manage how a file is uploaded.
   */
  function onRequestUploadFile(absoluteLink: string, relativeLink: string, onUploadFileCallback: (fileOid: number) => Promise<any>) {
    onOpenPopup({
      popup: 'uploadFile',
      absoluteLink,
      relativeLink,
      onUploadFileCallback,
      onClosePopup,
      onError,
    });
  }

  /**
   * Closes the current popup.
   */
  function onClosePopup() {
    setPopups((oldPopups) => oldPopups.splice(oldPopups.length - 1, 1));
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
      <div className="fixed left-0 right-0 top-0 bottom-0 grid grid-cols-[auto_1fr]">
        <Sidebar 
          selectedSchema={selectedSchema ? [selectedSchema[0], selectedSchema[1]] : null} 
          onSelectTable={(tableOid, tableName) => { setSelectedSchema(['table', tableOid, tableName]); }}
          onSelectReport={(reportOid, reportName) => { setSelectedSchema(['report', reportOid, reportName]); }}
          onRequestCreateTable={onRequestCreateTable} 
          onRequestCreateReport={onRequestCreateReport}
          onRequestEditTable={onRequestEditTable}
          onRequestEditReport={onRequestEditReport}
          onError={onError}
        />
        <Page
          schema={selectedSchema}
          onRequestCreateTableColumn={onRequestCreateTableColumn}
          onRequestEditTableColumn={onRequestEditTableColumn}
          onRequestUploadFile={onRequestUploadFile}
          onError={onError}
        />
      </div>
      <Popup popups={popups} />
      <Dialog open={err !== null} onOpenChange={(isOpen) => {
        if (!isOpen) {
          setErr(null);
        }
      }}>
          <Typography variant="h6">Error</Typography>
          <Dialog.Overlay>
            <Dialog.Content>
              <Typography variant="small">{err?.message}</Typography>
              <Typography variant="small">{err?.stack}</Typography>
            </Dialog.Content>
            <div className="mb-1 flex items-center justify-end gap-2">
              <Dialog.DismissTrigger as={Button} color="error">
                OK
              </Dialog.DismissTrigger>
            </div>
          </Dialog.Overlay>
      </Dialog>
    </main>
  );
}

export default App;
