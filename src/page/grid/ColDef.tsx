import { Button } from "@material-tailwind/react";

export type AddNewColumnButtonProps = {
    onClick: (e: React.MouseEvent) => any,
};
export function AddNewColumnButton(props: AddNewColumnButtonProps) {
    return (<div 
        className="absolute top-0 px-4 text-primary-foreground"
    >
        <Button
            className="text-nowrap"
            variant="gradient"
            onClick={props.onClick}
        >
            Add New Column
        </Button>
    </div>)
}