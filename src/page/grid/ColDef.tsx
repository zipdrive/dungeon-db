import { Button } from "@material-tailwind/react";

export type AddNewColumnButtonProps = {
    onClick: (e: React.MouseEvent) => any,
};
export function AddNewColumnButton(props: AddNewColumnButtonProps) {
    return (<Button
        variant="gradient"
        onClick={props.onClick}
    >
        Add New Column
    </Button>)
}