import { Dialog } from "@material-tailwind/react";
import { CreateTableColumnPopup, CreateTableColumnPopupProps } from "./CreateTableColumnPopup";
import { EditTableColumnPopup, EditTableColumnPopupProps } from "./EditTableColumnPopup";
import { useState, useEffect } from "react";
import { FilePopup, FilePopupProps } from "./FilePopup";
import { CreateTablePopup, CreateTablePopupProps } from "./CreateTablePopup";
import { CreateReportPopup, CreateReportPopupProps } from "./CreateReportPopup";
import { EditTablePopup, EditTablePopupProps } from "./EditTablePopup";
import { EditReportPopup, EditReportPopupProps } from "./EditReportPopup";

export type PopupBreadcrumb = 
    { popup: 'createTable' } & CreateTablePopupProps
    | { popup: 'createReport' } & CreateReportPopupProps
    | { popup: 'editTable' } & EditTablePopupProps
    | { popup: 'editReport' } & EditReportPopupProps
    | { popup: 'createTableColumn' } & CreateTableColumnPopupProps
    | { popup: 'editTableColumn' } & EditTableColumnPopupProps
    | { popup: 'uploadFile' } & FilePopupProps
;

export type PopupProps = {
    popups: PopupBreadcrumb[]
};

export function Popup(props: PopupProps): React.JSX.Element {
    return (<>
        {props.popups.map((popup, index) => {
            return (<Dialog key={`popup-${popup.popup}${index}`} open={true}>
                <Dialog.Overlay>
                    <Dialog.Content>
                        {popup.popup === 'createTable' && <CreateTablePopup {...popup} />}
                        {popup.popup === 'createReport' && <CreateReportPopup {...popup} />}
                        {popup.popup === 'editTable' && <EditTablePopup {...popup} />}
                        {popup.popup === 'editReport' && <EditReportPopup {...popup} />}
                        {popup.popup === 'createTableColumn' && <CreateTableColumnPopup {...popup} />}
                        {popup.popup === 'editTableColumn' && <EditTableColumnPopup {...popup} />}
                        {popup.popup === 'uploadFile' && <FilePopup {...popup} />}
                    </Dialog.Content>
                </Dialog.Overlay>
            </Dialog>)
        })}
    </>);
}