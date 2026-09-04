import { Button, Checkbox, Chip, Dialog, Input, Radio, Select, Tabs, Textarea, Typography } from "@material-tailwind/react";
import { useRef, useState } from "react";
import Multiselect from "../../components/Multiselect";

interface FormRootPropsNoTabs extends React.PropsWithChildren {
    title?: string,
}
interface FormRootPropsTabs {
    title?: string,
    tabs: { 
        value: string, 
        label: string,
        fields: (React.ReactNode | Iterable<React.ReactNode>)
    }[]
}
export type FormRootProps = FormRootPropsNoTabs | FormRootPropsTabs;


function FormRootWrapChildren(props: FormRootProps): React.JSX.Element {
    if ('tabs' in props) {
        return (<Tabs defaultValue={props.tabs.length > 0 ? props.tabs[0].value : undefined}>
            <Tabs.List>
                {props.tabs.map(({ value, label }) => (<Tabs.Trigger value={value}>{label}</Tabs.Trigger>))}
                <Tabs.TriggerIndicator />
            </Tabs.List>
            {props.tabs.map(({ value, fields }) => {
                return (<Tabs.Panel value={value} className="flex flex-col gap-y-6">
                    {fields}
                </Tabs.Panel>);
            })}
        </Tabs>);
    } else {
        return (<div className="flex flex-col gap-y-6">
            {props.children}
        </div>);
    }
}

/**
 * A form of fields.
 */
export function FormRoot(props: FormRootProps): React.JSX.Element {
    if (props.title) {
        return (<div className="flex flex-col gap-y-6">
            <Typography type="h4" className="text-center">{props.title}</Typography>
            <FormRootWrapChildren {...props} />
        </div>)
    } else {
        return (<FormRootWrapChildren {...props} />);
    }
}


export type FormTextFieldProps = {
    label: string,
    tooltip?: string,
    multiline?: boolean,
    value: string,
    onSetValue?: (newValue: string) => void,
};

/**
 * A text entry component on a form. 
 */
export function FormTextField(props: FormTextFieldProps): React.JSX.Element {
    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        {props.multiline 
            ? (<Textarea placeholder={props.label} value={props.value} onChange={(e) => { props.onSetValue?.(e.target.value); }} />)
            : (<Input placeholder={props.label} value={props.value} onChange={(e) => { props.onSetValue?.(e.target.value); }} />)
        }
    </div>);
}


export type FormIntegerFieldProps = {
    label: string,
    tooltip?: string,
    value: number,
    onSetValue?: (newValue: number) => void,
};

/**
 * A number entry component on a form. 
 */
export function FormIntegerField(props: FormIntegerFieldProps): React.JSX.Element {
    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        <Input placeholder={props.label} type="number" value={props.value} onChange={(e) => { props.onSetValue?.(parseInt(e.target.value)); }} />
    </div>);
}



export type FormRadioFieldProps<S extends string> = {
    label: string,
    name?: string,
    tooltip?: string,
    value: S | undefined,
    possibleValues: { value: S, label: string, tooltip?: string, disabled?: boolean }[],
    onSetValue?: (newValue: S) => void,
    orientation?: 'horizontal' | 'vertical',
};

/**
 * A set of radio buttons on a form.
 */
export function FormRadioField<S extends string>(props: FormRadioFieldProps<S>): React.JSX.Element {
    let name: string = '';
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    for (let i = 0; i < 10; i++) {
        name += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    name = props.name ?? name;

    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        <Radio 
            value={props.value} 
            orientation={props.orientation ?? 'horizontal'} 
            className="gap-x-6"
            onValueChange={(s) => { props.onSetValue?.(s as S); }}
        >
            {props.possibleValues.map((possibleValue) => {
                return (<div className="flex flex-row gap-x-2">
                    <Radio.Item 
                        id={name + possibleValue.value} 
                        value={possibleValue.value} 
                        disabled={possibleValue.disabled}
                        
                    >
                        <Radio.Indicator />
                    </Radio.Item>
                    <Typography as="label" htmlFor={name + possibleValue.value}>{possibleValue.label}</Typography>
                </div>);
            })}
        </Radio>
    </div>);
}



export type FormSelectFieldProps<S extends string> = {
    label: string,
    tooltip?: string,
    value: S | undefined,
    possibleValues: { value: S, label: string, disabled?: boolean }[],
    onSetValue?: (newValue: S) => void,
};

/**
 * A dropdown entry component on a form, where only one value can be selected.
 */
export function FormSelectField<S extends string>(props: FormSelectFieldProps<S>): React.JSX.Element {
    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        <Select value={props.value} onValueChange={(s) => { props.onSetValue?.(s as S); }}>
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
    </div>);
}



export type FormMultiselectFieldProps<S extends string> = {
    label: string,
    tooltip?: string,
    value: S[],
    possibleValues: { value: S, label: string, disabled?: boolean }[],
    onSetValue?: (newValue: S[]) => void,
};

/**
 * A dropdown entry component on a form, where multiple values can be selected.
 */
export function FormMultiselectField<S extends string>(props: FormMultiselectFieldProps<S>): React.JSX.Element {
    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        <Multiselect onValueChange={(s) => { props.onSetValue?.(s as S[]); }}>
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
    </div>);
}



export type FormCheckboxFieldProps = {
    label: string,
    tooltip?: string,
    value: boolean,
    onSetValue?: (newValue: boolean) => void,
};

/**
 * A checkbox entry component on a form.
 */
export function FormCheckboxField(props: FormCheckboxFieldProps): React.JSX.Element {
    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        <Checkbox checked={props.value} onChange={(e) => { props.onSetValue?.(e.target.checked); }} />
    </div>);
}



export type FormFormulaFieldProps = {
    label: string,
    tooltip?: string,
    value: string,
    onSetValue?: (newValue: string) => void,
};

export function FormFormulaField(props: FormFormulaFieldProps): React.JSX.Element {
    const textareaRef = useRef<HTMLTextAreaElement>(null);
    const [textareaCursor, setTextareaCursor] = useState<{ start: number, end: number }>({ start: 0, end: 0 });

    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        <Textarea ref={textareaRef} placeholder={props.label} onChange={(e) => { props.onSetValue?.(e.target.value); }}>
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
                                    props.onSetValue?.(
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
    </div>);
}



export interface FormCustomFieldProps extends React.PropsWithChildren {
    label: string
};

/**
 * A customized field in a form.
 */
export function FormCustomField(props: FormCustomFieldProps): React.JSX.Element {
    return (<div className="flex flex-col gap-y-2">
        <Typography type="h6">{props.label}</Typography>
        {props.children}
    </div>);
}



export const Form = Object.assign(FormRoot, {
    TextField: FormTextField,
    IntegerField: FormIntegerField,
    CheckboxField: FormCheckboxField,
    RadioField: FormRadioField,
    SelectField: FormSelectField,
    MultiselectField: FormMultiselectField,
    FormulaField: FormFormulaField,
    CustomField: FormCustomField,
});
export default Form;