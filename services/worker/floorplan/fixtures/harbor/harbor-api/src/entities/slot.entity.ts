import { Column, Entity, PrimaryColumn } from 'typeorm';

@Entity('slot')
export class Slot {
  @PrimaryColumn()
  id!: string;

  @Column()
  name!: string;
}
