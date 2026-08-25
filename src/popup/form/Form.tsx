import { Button, Checkbox, Chip, Dialog, Input, Select, Textarea, Typography } from "@material-tailwind/react";
import { useRef, useState } from "react";
import Multiselect from "../../components/Multiselect";

export interface FormRootProps extends React.PropsWithChildren {
    title?: string
}

/**
 * A form of fields.
 */
export function FormRoot(props: FormRootProps): React.JSX.Element {
    if (props.title) {
        return (<div>
            <Typography variant="h4">{props.title}</Typography>
            <div>
                {props.children}
            </div>
        </div>)
    } else {
        return (<div>
            {props.children}
        </div>);
    }
}


export type FormTextEntryProps = {
    label: string,
    tooltip?: string,
    value: string,
    onSetValue: (newValue: string) => void,
};

/**
 * A text entry component on a form. 
 */
export function FormTextField(props: FormTextEntryProps): React.JSX.Element {
    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Input placeholder={props.label} value={props.value} onChange={(e) => { props.onSetValue(e.target.value); }} />
    </>);
}



export type FormSelectEntryProps<S extends string> = {
    label: string,
    tooltip?: string,
    value: S | undefined,
    possibleValues: { value: S, label: string, disabled?: boolean }[],
    onSetValue: (newValue: S) => void,
};

/**
 * A dropdown entry component on a form.
 */
export function FormSelectField<S extends string>(props: FormSelectEntryProps<S>): React.JSX.Element {
    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Select onValueChange={(s) => { props.onSetValue(s as S); }}>
            <Select.Trigger placeholder={props.label}></Select.Trigger>
            <Select.List>
                {props.possibleValues.map(({ value: optionValue, label: optionLabel, disabled: optionDisabled }) => 
                    (<Select.Option 
                        value={optionValue as string}
                        disabled={optionDisabled ?? false}
                    >
                        {optionLabel}
                    </Select.Option>)
                )}
            </Select.List>
        </Select>
    </>);
}



export type FormMultiselectEntryProps<S extends string> = {
    label: string,
    tooltip?: string,
    value: S[],
    possibleValues: { value: S, label: string, disabled?: boolean }[],
    onSetValue: (newValue: S) => void,
};

/**
 * A dropdown entry component on a form.
 */
export function FormMultiselectField<S extends string>(props: FormMultiselectEntryProps<S>): React.JSX.Element {
    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Multiselect onValueChange={(s) => { props.onSetValue(s as S); }}>
            <Multiselect.Trigger placeholder={props.label}></Multiselect.Trigger>
            <Multiselect.List>
                {props.possibleValues.map(({ value: optionValue, label: optionLabel, disabled: optionDisabled }) => 
                    (<Multiselect.Option 
                        value={optionValue as string}
                        disabled={optionDisabled ?? false}
                    >
                        {optionLabel}
                    </Multiselect.Option>)
                )}
            </Multiselect.List>
        </Multiselect>
    </>);
}



export type FormCheckboxEntryProps = {
    label: string,
    tooltip?: string,
    value: boolean,
    onSetValue: (newValue: boolean) => void,
};

/**
 * A checkbox entry component on a form.
 */
export function FormCheckboxField(props: FormCheckboxEntryProps): React.JSX.Element {
    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Checkbox checked={props.value} onChange={(e) => { props.onSetValue(e.target.checked); }} />
    </>);
}



export type FormFormulaEntryProps = {
    label: string,
    tooltip?: string,
    value: string,
    onSetValue: (newValue: string) => void,
};

export function FormFormulaField(props: FormFormulaEntryProps): React.JSX.Element {
    const textareaRef = useRef<HTMLTextAreaElement>(null);
    const [textareaCursor, setTextareaCursor] = useState<{ start: number, end: number }>({ start: 0, end: 0 });

    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Textarea ref={textareaRef} placeholder={props.label} onChange={(e) => { props.onSetValue(e.target.value); }}>
            {props.value
                .split(/(@\{ROOT\d+(?:_INHERITOR\d+|_MASTER\d+|_COLUMN\d+)*_COLUMN\d+\})/gi)
                .reduce<React.ReactNode[]>((prev, current, i) => {
                    if (i % 2 == 1) {
                        // TODO replace this with a label for the parameter
                        return prev.concat(<Chip>{current}</Chip>)
                    } else {
                        return prev.concat(current);
                    }
                }, [])}
        </Textarea>
        <div className="flex flex-row justify-end gap-2">
            <Dialog>
                <Dialog.Trigger as={Button}>Insert Parameter</Dialog.Trigger>
                <Dialog.Overlay>
                    <Dialog.Content>
                        <div className="flex gap-y-2">
                            <Dialog.DismissTrigger
                                as={Button}
                                variant="ghost"
                                color="error"
                                className="mr-1"
                                onClick={() => { 
                                    if (textareaRef.current) {
                                        const currentTextareaCursor: { start: number, end: number } = { 
                                            start: textareaRef.current.selectionStart, 
                                            end: textareaRef.current.selectionEnd 
                                        };
                                        setTextareaCursor(currentTextareaCursor);
                                    }
                                }}
                            >
                                <span>Cancel</span>
                            </Dialog.DismissTrigger>
                            <Button
                                variant="gradient" 
                                onClick={() => {
                                    props.onSetValue(
                                        props.value.slice(0, textareaCursor.start)
                                        + `@{}`
                                        + props.value.slice(textareaCursor.end)
                                    );
                                    setTimeout(() => {
                                        if (textareaRef.current) {
                                            textareaRef.current.selectionStart = textareaCursor.start;
                                            textareaRef.current.selectionEnd = textareaCursor.end;
                                        }
                                    }, 0);
                                }}
                            >
                                <span>Confirm</span>
                            </Button>
                        </div>
                    </Dialog.Content>
                </Dialog.Overlay>
            </Dialog>
        </div>
    </>);
}



export const Form = Object.assign(FormRoot, {
    TextField: FormTextField,
    SelectField: FormSelectField,
    CheckboxField: FormCheckboxField,
    FormulaField: FormFormulaField,
});
export default Form;