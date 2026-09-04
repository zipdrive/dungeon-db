import { Button, Dialog, Typography } from "@material-tailwind/react";
import { uploadFileAsync } from "../api/query";
import { useCallback } from "react";

export type FilePopupProps = {
    absoluteLink: string,
    relativeLink: string,
    onUploadFileCallback: (fileOid: number) => Promise<any>,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function FilePopup(props: FilePopupProps): React.JSX.Element {
    const absoluteLinkCallback = useCallback(async (_e: React.MouseEvent) => {
        try {
            const fileOid: number = await uploadFileAsync({
                file: {
                    path: {
                        oid: 0,
                        path: props.absoluteLink
                    }
                },
                uploadFromPath: props.absoluteLink
            });
            await props.onUploadFileCallback(fileOid);
        } catch (e) {
            props.onError(e);
        } finally {
            props.onClosePopup();
        }
    }, [props.absoluteLink, props.onUploadFileCallback, props.onClosePopup, props.onError]);
    const relativeLinkCallback = useCallback(async (_e: React.MouseEvent) => {
        try {
            const fileOid: number = await uploadFileAsync({
                file: {
                    path: {
                        oid: 0,
                        path: props.relativeLink
                    }
                },
                uploadFromPath: props.relativeLink
            });
            await props.onUploadFileCallback(fileOid);
        } catch (e) {
            props.onError(e);
        } finally {
            props.onClosePopup();
        }
    }, [props.relativeLink, props.onUploadFileCallback, props.onClosePopup, props.onError]);
    const dataCallback = useCallback(async (_e: React.MouseEvent) => {
        try {
            const fileOid: number = await uploadFileAsync({
                file: {
                    blob: {
                        oid: 0
                    }
                },
                uploadFromPath: props.absoluteLink
            });
            await props.onUploadFileCallback(fileOid);
        } catch (e) {
            props.onError(e);
        } finally {
            props.onClosePopup();
        }
    }, [props.absoluteLink, props.onUploadFileCallback, props.onClosePopup, props.onError]);

    return (<div className="flex flex-col gap-y-6">
        <Typography>
            Click&nbsp;<span style={{fontWeight: 'bold'}}>Absolute Link</span>&nbsp;to upload a link to the file as the following path:&nbsp;<span>{props.absoluteLink}</span>
        </Typography>
        <Typography>
            Click&nbsp;<span style={{fontWeight: 'bold'}}>Relative Link</span>&nbsp;to upload a link to the file as the following path:&nbsp;<span>{props.relativeLink}</span>
        </Typography>
        <Typography>
            Click&nbsp;<span style={{fontWeight: 'bold'}}>Data</span>&nbsp;to directly upload the data into the DungeonDB file. This will expand your DungeonDB file size by the size of the uploaded file, but you will continue to be able to access the data from DungeonDB even if you move or delete the original file.
        </Typography>
        <div className="flex flex-row justify-end gap-2">
            <Dialog.DismissTrigger
                as={Button}
                variant="ghost"
                color="error"
                onClick={() => {
                    props.onClosePopup();
                }}
                className="mr-1"
            >
                <span>Cancel</span>
            </Dialog.DismissTrigger>
            <Button
                variant="gradient" 
                onClick={absoluteLinkCallback}
            >
                <span>Absolute Link</span>
            </Button>
            <Button
                variant="gradient" 
                onClick={relativeLinkCallback}
            >
                <span>Relative Link</span>
            </Button>
            <Button
                variant="gradient" 
                onClick={dataCallback}
            >
                <span>Data</span>
            </Button>
        </div>
    </div>);
}