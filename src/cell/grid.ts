import { Menu } from "@tauri-apps/api/menu";
import { SchemaRow } from "../api/model/cell";
import { FullMetadata as ColumnFullMetadata } from "../api/model/column";
import { executeAsync } from "../api/action";
import { LogicalPosition } from "@tauri-apps/api/dpi";

export async function columnContextMenu(e: MouseEvent, columnMetadata: ColumnFullMetadata, onRequestEditColumn: (columnMetadata: ColumnFullMetadata) => void, onError: (e: unknown) => void) {
    const menu = await Menu.new({
        items: [
            {
                text: 'Edit',
                action: () => {
                    onRequestEditColumn(columnMetadata);
                }
            },
            {
                text: 'Delete',
                action: async () => {
                    try {
                        await executeAsync({
                            trashColumn: {
                                schemaOid: columnMetadata.schema.oid,
                                columnOid: columnMetadata.oid
                            }
                        });
                    } catch (e) {
                        onError(e);
                    }
                }
            }
        ]
    });

    menu.popup(new LogicalPosition(e.pageX, e.pageY));
}

export async function rowContextMenu(e: MouseEvent, rowMetadata: SchemaRow) {

}