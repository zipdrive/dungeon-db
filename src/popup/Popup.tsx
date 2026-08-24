import { Dialog } from "@material-tailwind/react";
import { CreateColumnPopup, CreateColumnPopupProps } from "./CreateColumnPopup";
import { CreateSchemaPopup, CreateSchemaPopupProps } from "./CreateSchemaPopup";
import { EditColumnPopup, EditColumnPopupProps } from "./EditColumnPopup";
import { EditSchemaPopup, EditSchemaPopupProps } from "./EditSchemaPopup";
import { useState, useEffect } from "react";

export type PopupProps = 
{ popup: 'none' }
| { popup: 'createSchema' } & CreateSchemaPopupProps
| { popup: 'editSchema' } & EditSchemaPopupProps
| { popup: 'createColumn' } & CreateColumnPopupProps
| { popup: 'editColumn' } & EditColumnPopupProps;

export function Popup(props: PopupProps): React.JSX.Element {
    const [lastKnownCreateSchemaPopupProps, setCreateSchemaPopupProps] = useState<CreateSchemaPopupProps | null>(null);
    const [lastKnownEditSchemaPopupProps, setEditSchemaPopupProps] = useState<EditSchemaPopupProps | null>(null);
    const [lastKnownCreateColumnPopupProps, setCreateColumnPopupProps] = useState<CreateColumnPopupProps | null>(null);
    const [lastKnownEditColumnPopupProps, setEditColumnPopupProps] = useState<EditColumnPopupProps | null>(null);

    useEffect(() => {
        switch (props.popup) {
            case 'createSchema':
                setCreateSchemaPopupProps(props);
                break;
            case 'editSchema':
                setEditSchemaPopupProps(props);
                break;
            case 'createColumn':
                setCreateColumnPopupProps(props);
                break;
            case 'editColumn':
                setEditColumnPopupProps(props);
                break;
        }
    }, [props.popup]);

    return (<>
        {lastKnownCreateSchemaPopupProps && <Dialog open={props.popup === 'createSchema'} handler={lastKnownCreateSchemaPopupProps.onClosePopup}>
            <CreateSchemaPopup {...lastKnownCreateSchemaPopupProps} isOpen={props.popup === 'createSchema'} />
        </Dialog>}
        {lastKnownEditSchemaPopupProps && <Dialog open={props.popup === 'editSchema'} handler={lastKnownEditSchemaPopupProps.onClosePopup}>
            <EditSchemaPopup {...lastKnownEditSchemaPopupProps} />
        </Dialog>}
        {lastKnownCreateColumnPopupProps && <Dialog open={props.popup === 'createColumn'} handler={lastKnownCreateColumnPopupProps.onClosePopup}>
            <CreateColumnPopup {...lastKnownCreateColumnPopupProps} isOpen={props.popup === 'createColumn'} />    
        </Dialog>}
        {lastKnownEditColumnPopupProps && <Dialog open={props.popup === 'editColumn'} handler={lastKnownEditColumnPopupProps.onClosePopup}>
            <EditColumnPopup {...lastKnownEditColumnPopupProps} />   
        </Dialog>}
    </>);
}